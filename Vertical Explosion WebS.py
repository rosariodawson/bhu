#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Vertical Explosion Catcher v5.11 — Multi-Stream Engine
---------------------------------------------------------------------------------
Solves 418 bans and WebSocket connection errors.

What’s new in this patch
- HOTFIX: Solves WSServerHandshakeError by "chunking" symbols into multiple
  WebSocket connections (300 symbols each) to stay under the 1024 stream limit.
- HOTFIX: Solves "Session is closed" error by correctly managing the
  aiohttp.ClientSession for the persistent Safe Poller task.
- Re-added the missing 'now_iso_utc' helper function.
- This script is a 24/7 daemon.
"""

import asyncio
import aiohttp
import sys
import json
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Set

# ----------------------------- Logging helpers -----------------------------
import logging

def make_logger()->logging.Logger:
    logger = logging.getLogger("vec511")
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    h.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(h)
    return logger

log = make_logger()

def http_log(method:str, url:str, status:int):
    # Log non-404 client errors and all server errors
    if status >= 500 or (400 <= status < 404) or (status > 404):
        log.info('HTTP Request: %s %s "HTTP/1.1 %s"', method, url, status)

# ----------------------------- Endpoints -----------------------------
BINANCE_WS_FUTURES_COMBINED = "wss://fstream.binance.com/stream?streams="

# REST endpoints for Safe Poller
FUT_EXCHANGE_INFO  = "https://fapi.binance.com/fapi/v1/exchangeInfo"
SPOT_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT"
FUT_24HR           = "https://fapi.binance.com/fapi/v1/ticker/24hr"
SPOT_24HR          = "https://api.binance.com/api/v3/ticker/24hr"
FUT_KLINES         = "https://fapi.binance.com/fapi/v1/klines" # For PDH/PDL
FUT_FUNDING        = "https://fapi.binance.com/fapi/v1/fundingRate"
FUT_TAKER_LS       = "https://fapi.binance.com/futures/data/takerlongshortRatio"
FUT_GLSR           = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
FUT_OI_HIST        = "https://fapi.binance.com/fapi/v1/openInterestHist"
FUT_AGG_TRADES     = "https://fapi.binance.com/fapi/v1/aggTrades" # For CVD
FUT_DEPTH          = "https://fapi.binance.com/fapi/v1/depth"   # For Magnetic TPs

# Bybit
BYBIT_FUT_INFO   = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_OI_HIST    = "https://api.bybit.com/v5/market/open-interest"

# Bitget
BITGET_FUT_INFO  = "https://api.bitget.com/api/v2/mix/market/contracts"
BITGET_OI_HIST   = "https://api.bitget.com/api/v2/mix/market/history-open-interest"

HEADERS = {"Accept": "application/json", "User-Agent": "vec511/multi-stream"}
HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)

# ----------------------------- Real-Time State Manager -----------------------------
class MarketState:
    def __init__(self, symbol):
        self.symbol = symbol
        self.current_price = 0.0
        self.kline_30m = [] # List of 6 5m klines
        self.cvd_slope = 0.0
        self.ask_walls = {} # {price: size}
        self.bid_walls = {} # {price: size}
        self.price_chg_24h = 0.0
        self.spot_taker_ratio = 0.5
        self.fut_taker_ratio = 0.5
        self.funding_rate = 0.0
        self.glsr = "glsr_weak"
        self.oi_spike_pct = 0.0
        self.oi_source = "None"
        self.pdh = 0.0
        self.pdl = 0.0
    
    def update_kline(self, kline_data):
        self.kline_30m.append(kline_data)
        if len(self.kline_30m) > 6:
            self.kline_30m.pop(0)
    
    def get_price_change_30m(self) -> Optional[float]:
        if len(self.kline_30m) < 6: return None
        try:
            start_price = safe_float(self.kline_30m[0]['o'])
            end_price = safe_float(self.kline_30m[5]['c'])
            if start_price == 0: return None
            return ((end_price / start_price) - 1.0) * 100.0
        except Exception:
            return None

    def update_cvd(self, trade):
        try:
            price = safe_float(trade.get("p"))
            qty = safe_float(trade.get("q"))
            notional = price * qty
            is_buyer_maker = trade.get("m", False)
            if is_buyer_maker: self.cvd_slope -= notional
            else: self.cvd_slope += notional
            self.current_price = price
        except Exception as e:
            log.warning("%s: Error updating CVD: %s", self.symbol, e)

    def update_depth(self, depth_data, wall_threshold=500000):
        for price_str, qty_str in depth_data.get("a", []): # Asks
            price = safe_float(price_str)
            notional = price * safe_float(qty_str)
            if notional > wall_threshold: self.ask_walls[price] = notional
            elif price in self.ask_walls: del self.ask_walls[price]
                
        for price_str, qty_str in depth_data.get("b", []): # Bids
            price = safe_float(price_str)
            notional = price * safe_float(qty_str)
            if notional > wall_threshold: self.bid_walls[price] = notional
            elif price in self.bid_walls: del self.bid_walls[price]

GLOBAL_STATE: Dict[str, MarketState] = {}

# ----------------------------- Utils -----------------------------

def now_iso_utc()->str:
    """[FIXED] Returns the current UTC time in ISO format."""
    return datetime.now(timezone.utc).isoformat()

def safe_float(x: Any, default: float = 0.0) -> float:
    try: return float(x)
    except Exception: return default

def normalize_symbol(s: str) -> str:
    return s.strip().upper()

# ----------------------------- Data Fetchers (For Safe Poller) -----------------------------

async def fetch_json(session: aiohttp.ClientSession, method: str, url: str, params: Optional[Dict[str, Any]]=None) -> Tuple[Optional[Any], int]:
    try:
        async with session.request(method, url, params=params, timeout=HTTP_TIMEOUT) as resp:
            status = resp.status
            # Log non-404 client errors and all server errors
            if status >= 500 or (400 <= status < 404) or (status > 404):
                 http_log(method.upper(), str(resp.url), status)
            if 200 <= status < 300:
                return await resp.json(content_type=None), status
            else:
                return None, status
    except asyncio.TimeoutError:
        log.warning("Request timeout %s %s", method, url)
        return None, -1
    except aiohttp.ClientError as e:
        log.warning("Request error %s %s params=%s err=%s", method, url, params, e)
        return None, -1
    except Exception as e:
        log.error("Unhandled error in fetch_json: %s", e, exc_info=True)
        return None, -1


async def load_all_exchange_maps(session: aiohttp.ClientSession) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Set[str], Set[str]]:
    """Returns (binance_fut_map, binance_spot_map, bybit_map, bitget_map)"""
    # --- Binance ---
    fut_json, status = await fetch_json(session, "GET", FUT_EXCHANGE_INFO)
    if status != 200:
        log.error(f"CRITICAL: Failed to fetch Binance exchange info (Status: {status}). Check IP ban (418) or network.")
        return {}, {}, set(), set()
        
    spot_json, _ = await fetch_json(session, "GET", SPOT_EXCHANGE_INFO)
    fut_map: Dict[str, Dict[str, Any]] = {}
    spot_map: Dict[str, Dict[str, Any]] = {}
    if fut_json and isinstance(fut_json, dict):
        for s in fut_json.get("symbols", []):
            sym = s.get("symbol")
            if sym: fut_map[sym] = {"symbol": sym, "pair": s.get("pair"), "status": s.get("status")}
    if spot_json and isinstance(spot_json, dict):
        for s in spot_json.get("symbols", []):
            sym = s.get("symbol")
            if sym: spot_map[sym] = {"symbol": sym, "status": s.get("status")}
    log.info("Fetched %d Binance futures and %d Binance spot symbols.", len(fut_map), len(spot_map))
    
    # --- Bybit ---
    bybit_map = set()
    try:
        bybit_js, _ = await fetch_json(session, "GET", BYBIT_FUT_INFO, params={"category": "linear"})
        if bybit_js and bybit_js.get("result"):
            for s in bybit_js["result"].get("list", []):
                if s.get("symbol"): bybit_map.add(s["symbol"])
        log.info("Fetched %d Bybit linear futures.", len(bybit_map))
    except Exception as e:
        log.error("Failed to fetch Bybit markets: %s", e)
        
    # --- Bitget ---
    bitget_map = set()
    try:
        bitget_js, _ = await fetch_json(session, "GET", BITGET_FUT_INFO, params={"productType": "USDT-FUTURES"})
        if bitget_js and bitget_js.get("data"):
            for s in bitget_js["data"]:
                if s.get("symbol"): bitget_map.add(s["symbol"])
        log.info("Fetched %d Bitget linear futures.", len(bitget_map))
    except Exception as e:
        log.error("Failed to fetch Bitget markets: %s", e)

    return fut_map, spot_map, bybit_map, bitget_map


def futures_to_spot_symbol(fut_info: Dict[str, Any], spot_map: Dict[str, Dict[str, Any]]) -> Optional[str]:
    spot_candidate = fut_info.get("pair")
    if not spot_candidate: return None
    if spot_candidate in spot_map: return spot_candidate
    return None

async def get_price_change_percent_24h(session: aiohttp.ClientSession, fut_symbol: str) -> Optional[float]:
    js, _ = await fetch_json(session, "GET", FUT_24HR, params={"symbol": fut_symbol})
    if js: return safe_float(js.get("priceChangePercent"), None)
    return None

async def get_spot_taker_ratio(session: aiohttp.ClientSession, spot_symbol: str) -> Optional[float]:
    js, _ = await fetch_json(session, "GET", SPOT_24HR, params={"symbol": spot_symbol})
    if not js: return None
    taker_buy = safe_float(js.get("takerBuyBaseAssetVolume"))
    vol = max(safe_float(js.get("volume")), 1e-12)
    return max(0.0, min(1.0, taker_buy / vol))

async def get_futures_taker_ratio(session: aiohttp.ClientSession, fut_symbol: str) -> Optional[float]:
    js, status = await fetch_json(session, "GET", FUT_TAKER_LS, params={"symbol": fut_symbol, "period": "5m", "limit": 1})
    if not js or not isinstance(js, list) or not js: return None
    r = safe_float(js[-1].get("buySellRatio"))
    if r <= 0: return 0.0
    return r / (1.0 + r)

async def get_avg_funding_raw(session: aiohttp.ClientSession, fut_symbol: str, limit: int = 21) -> Optional[float]:
    js, _ = await fetch_json(session, "GET", FUT_FUNDING, params={"symbol": fut_symbol, "limit": limit})
    if not js or not isinstance(js, list) or not js: return None
    vals = [safe_float(x.get("fundingRate")) for x in js if "fundingRate" in x]
    if not vals: return None
    return sum(vals)/len(vals)

async def get_glsr_class(session: aiohttp.ClientSession, fut_symbol: str) -> str:
    js, _ = await fetch_json(session, "GET", FUT_GLSR, params={"symbol": fut_symbol, "period": "5m", "limit": 50})
    if not js or not isinstance(js, list) or not js: return "glsr_weak"
    last = js[-1]
    ratio = safe_float(last.get("longShortRatio"), 0.5)
    if 0.48 <= ratio <= 0.52: return "glsr_weak"
    return "glsr_ok"

# --- OI Spike Functions ('scalp' mode) ---
async def get_oi_spike_percent_binance_scalp(session: aiohttp.ClientSession, fut_symbol: str) -> float:
    js, _ = await fetch_json(session, "GET", FUT_OI_HIST, params={"symbol": fut_symbol, "period": "5m", "limit": 8}) # 8 * 5m = 40 mins
    if not js or not isinstance(js, list) or len(js) < 8: return 0.0 
    try:
        baseline_data = [safe_float(d.get("sumOpenInterestValue")) for d in js[0:6]] # First 30 mins
        recent_data = [safe_float(d.get("sumOpenInterestValue")) for d in js[6:8]]   # Last 10 mins
        if not baseline_data or not recent_data: return 0.0
        baseline_avg = sum(baseline_data) / len(baseline_data); recent_avg = sum(recent_data) / len(recent_data)
        if baseline_avg == 0: return 0.0
        return max(0.0, ((recent_avg / baseline_avg) - 1.0) * 100.0)
    except Exception as e:
        log.warning("%s (Binance-Scalp): Error calculating OI spike: %s", fut_symbol, e); return 0.0

async def get_oi_spike_percent_bybit_scalp(session: aiohttp.ClientSession, fut_symbol: str) -> float:
    params = {"category": "linear", "symbol": fut_symbol, "intervalTime": "5min", "limit": 8}
    js, _ = await fetch_json(session, "GET", BYBIT_OI_HIST, params=params)
    if not js or not js.get("result") or not js["result"].get("list"): return 0.0
    data_list = js["result"]["list"];
    if len(data_list) < 8: return 0.0
    try:
        data_list.reverse(); baseline_data = [safe_float(d.get("openInterest")) for d in data_list[0:6]]
        recent_data = [safe_float(d.get("openInterest")) for d in data_list[6:8]]
        if not baseline_data or not recent_data: return 0.0
        baseline_avg = sum(baseline_data) / len(baseline_data); recent_avg = sum(recent_data) / len(recent_data)
        if baseline_avg == 0: return 0.0
        return max(0.0, ((recent_avg / baseline_avg) - 1.0) * 100.0)
    except Exception as e:
        log.warning("%s (Bybit-Scalp): Error calculating OI spike: %s", fut_symbol, e); return 0.0

async def get_oi_spike_percent_bitget_scalp(session: aiohttp.ClientSession, fut_symbol: str) -> float:
    bitget_symbol = f"{fut_symbol}_UMCBL"; params = {"symbol": bitget_symbol, "interval": "5m", "limit": 8}
    js, _ = await fetch_json(session, "GET", BITGET_OI_HIST, params=params)
    if not js or not js.get("data"): return 0.0
    data_list = js["data"];
    if len(data_list) < 8: return 0.0
    try:
        data_list.reverse(); baseline_data = [safe_float(d.get("openInterest")) for d in data_list[0:6]]
        recent_data = [safe_float(d.get("openInterest")) for d in data_list[6:8]]
        if not baseline_data or not recent_data: return 0.0
        baseline_avg = sum(baseline_data) / len(baseline_data); recent_avg = sum(recent_data) / len(recent_data)
        if baseline_avg == 0: return 0.0
        return max(0.0, ((recent_avg / baseline_avg) - 1.0) * 100.0)
    except Exception as e:
        log.warning("%s (Bitget-Scalp): Error calculating OI spike: %s", fut_symbol, e); return 0.0

# --- OI Spike Functions ('hold' mode) ---
async def get_oi_spike_percent_binance_hold(session: aiohttp.ClientSession, fut_symbol: str) -> float:
    js, _ = await fetch_json(session, "GET", FUT_OI_HIST, params={"symbol": fut_symbol, "period": "5m", "limit": 24})
    if not js or not isinstance(js, list) or len(js) < 24: return 0.0 
    try:
        baseline_data = [safe_float(d.get("sumOpenInterestValue")) for d in js[0:20]]
        recent_data = [safe_float(d.get("sumOpenInterestValue")) for d in js[20:24]]
        if not baseline_data or not recent_data: return 0.0
        baseline_avg = sum(baseline_data) / len(baseline_data); recent_avg = sum(recent_data) / len(recent_data)
        if baseline_avg == 0: return 0.0
        return max(0.0, ((recent_avg / baseline_avg) - 1.0) * 100.0)
    except Exception as e:
        log.warning("%s (Binance-Hold): Error calculating OI spike: %s", fut_symbol, e); return 0.0

async def get_oi_spike_percent_bybit_hold(session: aiohttp.ClientSession, fut_symbol: str) -> float:
    params = {"category": "linear", "symbol": fut_symbol, "intervalTime": "5min", "limit": 24}
    js, _ = await fetch_json(session, "GET", BYBIT_OI_HIST, params=params)
    if not js or not js.get("result") or not js["result"].get("list"): return 0.0
    data_list = js["result"]["list"];
    if len(data_list) < 24: return 0.0
    try:
        data_list.reverse(); baseline_data = [safe_float(d.get("openInterest")) for d in data_list[0:20]]
        recent_data = [safe_float(d.get("openInterest")) for d in data_list[20:24]]
        if not baseline_data or not recent_data: return 0.0
        baseline_avg = sum(baseline_data) / len(baseline_data); recent_avg = sum(recent_data) / len(recent_data)
        if baseline_avg == 0: return 0.0
        return max(0.0, ((recent_avg / baseline_avg) - 1.0) * 100.0)
    except Exception as e:
        log.warning("%s (Bybit-Hold): Error calculating OI spike: %s", fut_symbol, e); return 0.0

async def get_oi_spike_percent_bitget_hold(session: aiohttp.ClientSession, fut_symbol: str) -> float:
    bitget_symbol = f"{fut_symbol}_UMCBL"; params = {"symbol": bitget_symbol, "interval": "5m", "limit": 24}
    js, _ = await fetch_json(session, "GET", BITGET_OI_HIST, params=params)
    if not js or not js.get("data"): return 0.0
    data_list = js["data"];
    if len(data_list) < 24: return 0.0
    try:
        data_list.reverse(); baseline_data = [safe_float(d.get("openInterest")) for d in data_list[0:20]]
        recent_data = [safe_float(d.get("openInterest")) for d in data_list[20:24]]
        if not baseline_data or not recent_data: return 0.0
        baseline_avg = sum(baseline_data) / len(baseline_data); recent_avg = sum(recent_data) / len(recent_data)
        if baseline_avg == 0: return 0.0
        return max(0.0, ((recent_avg / baseline_avg) - 1.0) * 100.0)
    except Exception as e:
        log.warning("%s (Bitget-Hold): Error calculating OI spike: %s", fut_symbol, e); return 0.0

# ----------------------------- Filters (Scoring Logic) -----------------------------

def evaluate_m1_score_scalp(
    price_chg_30m: Optional[float],
    spot_taker: Optional[float],
    fut_taker: Optional[float],
    avg_funding_raw: Optional[float],
    oi_spike_pct: float,
    cvd_slope: float
) -> Tuple[float, str]:
    """SCALP: 14-point signal scoring system based on 30m price change."""
    score = 0.0; synopsis_notes = []; CVD_STRONG_THRESHOLD = 250000
    if price_chg_30m is not None:
        if price_chg_30m > 0.5: score += 1.0; synopsis_notes.append("Δ30m>0.5%")
        if price_chg_30m > 2.0: score += 2.0; synopsis_notes.append("Δ30m>2%!!")
        if price_chg_30m < 10.0: score += 1.0
    is_spot_bullish = (spot_taker is not None and spot_taker > 0.52)
    is_fut_bullish = (fut_taker is not None and fut_taker > 0.52)
    if is_fut_bullish: score += 1.0; synopsis_notes.append("FutTkr")
    if is_spot_bullish: score += 1.0; synopsis_notes.append("SpotTkr")
    if is_fut_bullish and is_spot_bullish: score += 2.0; synopsis_notes.append("TkrAlign")
    if avg_funding_raw is not None:
        if avg_funding_raw < 0.0001: score += 1.0; synopsis_notes.append("FundOK")
        if avg_funding_raw < -0.0001: score += 1.0; synopsis_notes.append("FundNeg🔥")
    if oi_spike_pct > 15.0: score += 1.0; synopsis_notes.append("OI>15%")
    if oi_spike_pct > 30.0: score += 1.0; synopsis_notes.append("OI>30%!")
    if cvd_slope > 0: score += 1.0; synopsis_notes.append("CVD_Pos")
    if cvd_slope > CVD_STRONG_THRESHOLD: score += 1.0; synopsis_notes.append("CVD_Str🔥")
    return score, " | ".join(synopsis_notes)

def evaluate_m1_score_hold(
    price_chg_pct_24h: Optional[float],
    spot_taker: Optional[float],
    fut_taker: Optional[float],
    avg_funding_raw: Optional[float],
    oi_spike_pct: float,
    cvd_slope: float
) -> Tuple[float, str]:
    """HOLD: 14-point signal scoring system based on 24h price change."""
    score = 0.0; synopsis_notes = []; CVD_STRONG_THRESHOLD = 250000
    if price_chg_pct_24h is not None:
        if price_chg_pct_24h > 0: score += 1.0; synopsis_notes.append("Δ>0")
        if price_chg_pct_24h > 5.0: score += 2.0; synopsis_notes.append("Δ>5%")
        if price_chg_pct_24h < 50.0: score += 1.0
    is_spot_bullish = (spot_taker is not None and spot_taker > 0.52)
    is_fut_bullish = (fut_taker is not None and fut_taker > 0.52)
    if is_fut_bullish: score += 1.0; synopsis_notes.append("FutTkr")
    if is_spot_bullish: score += 1.0; synopsis_notes.append("SpotTkr")
    if is_fut_bullish and is_spot_bullish: score += 2.0; synopsis_notes.append("TkrAlign")
    if avg_funding_raw is not None:
        if avg_funding_raw < 0.0001: score += 1.0; synopsis_notes.append("FundOK")
        if avg_funding_raw < -0.0001: score += 1.0; synopsis_notes.append("FundNeg🔥")
    if oi_spike_pct > 15.0: score += 1.0; synopsis_notes.append("OI>15%")
    if oi_spike_pct > 30.0: score += 1.0; synopsis_notes.append("OI>30%!")
    if cvd_slope > 0: score += 1.0; synopsis_notes.append("CVD_Pos")
    if cvd_slope > CVD_STRONG_THRESHOLD: score += 1.0; synopsis_notes.append("CVD_Str🔥")
    return score, " | ".join(synopsis_notes)

# ----------------------------- New: Safe Poller Task -----------------------------

async def safe_poller_task(
    session: aiohttp.ClientSession,
    spot_map: Dict[str, Dict[str, Any]],
    bybit_map: Set[str],
    bitget_map: Set[str],
    mode: str
):
    """
    Runs on a slow loop (5 mins) to safely fetch REST-only data
    and update the GLOBAL_STATE.
    """
    global fut_map # Use the global map
    
    while True:
        log.info("SAFE POLLER: Starting 5-minute data poll...")
        try:
            for symbol in GLOBAL_STATE.keys():
                if symbol not in fut_map: continue
                state = GLOBAL_STATE[symbol]
                
                # --- Fetch Polled Data Concurrently ---
                (
                    price_24h, fut_taker, glsr_class, funding_raw, pd_kline, spot_taker
                ) = await asyncio.gather(
                    get_price_change_percent_24h(session, symbol),
                    get_futures_taker_ratio(session, symbol),
                    get_glsr_class(session, symbol),
                    get_avg_funding_raw(session, symbol),
                    fetch_json(session, "GET", FUT_KLINES, {"symbol": symbol, "interval": "1d", "limit": 2}),
                    get_spot_taker_ratio(session, spot_map[fut_map[symbol]["pair"]]["symbol"]) if fut_map[symbol]["pair"] in spot_map else asyncio.sleep(0, result=0.5)
                )

                # --- Update State ---
                state.price_chg_24h = price_24h
                state.fut_taker_ratio = fut_taker
                state.glsr = glsr_class
                state.funding_rate = funding_raw
                state.spot_taker_ratio = spot_taker
                
                if pd_kline[0] and isinstance(pd_kline[0], list) and len(pd_kline[0]) >= 2:
                    state.pdh = safe_float(pd_kline[0][0][2]) # Prev Day High
                    state.pdl = safe_float(pd_kline[0][0][3]) # Prev Day Low

                # --- OI Fallback Logic ---
                oi_source = "None"; oi_spike_pct = 0.0
                if mode == 'scalp':
                    oi_spike_pct = await get_oi_spike_percent_binance_scalp(session, symbol)
                    if oi_spike_pct > 0: oi_source = "Binance"
                    elif symbol in bybit_map:
                        oi_spike_pct = await get_oi_spike_percent_bybit_scalp(session, symbol)
                        if oi_spike_pct > 0: oi_source = "Bybit"
                    bitget_symbol = f"{symbol}_UMCBL"
                    if oi_source == "None" and bitget_symbol in bitget_map:
                        oi_spike_pct = await get_oi_spike_percent_bitget_scalp(session, symbol)
                        if oi_spike_pct > 0: oi_source = "Bitget"
                else: # mode == 'hold'
                    oi_spike_pct = await get_oi_spike_percent_binance_hold(session, symbol)
                    if oi_spike_pct > 0: oi_source = "Binance"
                    elif symbol in bybit_map:
                        oi_spike_pct = await get_oi_spike_percent_bybit_hold(session, symbol)
                        if oi_spike_pct > 0: oi_source = "Bybit"
                    bitget_symbol = f"{symbol}_UMCBL"
                    if oi_source == "None" and bitget_symbol in bitget_map:
                        oi_spike_pct = await get_oi_spike_percent_bitget_hold(session, symbol)
                        if oi_spike_pct > 0: oi_source = "Bitget"
                
                state.oi_spike_pct = oi_spike_pct
                state.oi_source = oi_source
                
                await asyncio.sleep(0.5) # CRITICAL: Sleep between symbols to avoid 418 ban

        except Exception as e:
            log.error(f"SAFE POLLER: Error during poll: {e}", exc_info=True)
            
        log.info("SAFE POLLER: Poll finished. Sleeping for 5 minutes.")
        await asyncio.sleep(300) # Sleep for 5 minutes

# ----------------------------- New: WebSocket Manager -----------------------------

async def stream_data(symbols: List[str], mode: str, min_score: float, name: str):
    """
    Connects to Binance combined streams and processes live data.
    This is the new "main loop" of the script.
    """
    streams = []
    for s in symbols:
        s_low = s.lower()
        streams.append(f"{s_low}@kline_5m")
        streams.append(f"{s_low}@aggTrade")
        streams.append(f"{s_low}@depth5@100ms")
        
    url = BINANCE_WS_FUTURES_COMBINED + "/".join(streams)
    log.info(f"[{name}] Connecting to WebSocket with {len(streams)} streams...")
    
    while True: # Add eternal reconnect loop
        try:
            async with aiohttp.ClientSession().ws_connect(url, timeout=None, heartbeat=60) as ws:
                log.info(f"[{name}] WebSocket Connection SUCCESSFUL.")
                
                while True: # Inner message processing loop
                    msg_raw = await ws.receive()
                    
                    if msg_raw.type == aiohttp.WSMsgType.TEXT:
                        msg = json.loads(msg_raw.data)
                        
                        if "stream" not in msg or "data" not in msg:
                            continue
                            
                        stream_name = msg["stream"]
                        data = msg["data"]
                        
                        symbol = stream_name.split('@')[0].upper()
                        if symbol not in GLOBAL_STATE:
                            continue
                        
                        state = GLOBAL_STATE[symbol]
                        
                        if stream_name.endswith("@kline_5m"):
                            kline = data['k']
                            if kline['x']: # 'x' is True if the kline is closed
                                log.info(f"[{name}] KLINE CLOSED: {symbol}")
                                state.update_kline(kline)
                                await evaluate_signal(state, mode, min_score) # Run signal logic
                        
                        elif stream_name.endswith("@aggTrade"):
                            state.update_cvd(data)
                        
                        elif stream_name.endswith("@depth5@100ms"):
                            state.update_depth(data)
                            
                    elif msg_raw.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        log.warning(f"[{name}] WebSocket closed or errored. Breaking to reconnect.")
                        break # Break inner loop to trigger reconnect
                            
        except Exception as e:
            log.error(f"[{name}] WebSocket Error: {e}. Reconnecting in 30 seconds...")
            await asyncio.sleep(30)

# ----------------------------- New: Signal Evaluator -----------------------------

async def evaluate_signal(state: MarketState, mode: str, min_score: float):
    """
    Runs the scoring model for a single symbol when its 5m kline closes.
    """
    
    m1_score = 0.0
    synopsis = ""
    price_chg_log = state.price_chg_24h

    # --- Run Mode-Specific Scoring ---
    if mode == 'scalp':
        price_chg_30m = state.get_price_change_30m()
        if price_chg_30m is None:
            log.debug(f"{state.symbol}: Skipping, not enough 30m kline data.")
            return # Not enough data yet
        
        price_chg_log = price_chg_30m
        m1_score, synopsis = evaluate_m1_score_scalp(
            price_chg_30m, state.spot_taker_ratio, state.fut_taker_ratio,
            state.funding_rate, state.oi_spike_pct, state.cvd_slope
        )

    elif mode == 'hold':
        m1_score, synopsis = evaluate_m1_score_hold(
            state.price_chg_24h, state.spot_taker_ratio, state.fut_taker_ratio,
            state.funding_rate, state.oi_spike_pct, state.cvd_slope
        )

    # --- Filter ---
    if m1_score < min_score:
        return # Did not pass filter

    # --- Build Trade Plan ---
    entry_price = state.current_price
    sl_price = None
    
    live_bid_walls = sorted([w for w in state.bid_walls.keys() if w < entry_price], reverse=True)
    if live_bid_walls:
        sl_price = live_bid_walls[0]
    else:
        sl_price = state.pdl

    resistance_targets = sorted(list(set(
        [w for w in state.ask_walls.keys() if w > entry_price] + [state.pdh]
    )))
    
    tp1 = resistance_targets[0] if len(resistance_targets) > 0 and resistance_targets[0] is not None else None
    tp2 = resistance_targets[1] if len(resistance_targets) > 1 and resistance_targets[1] is not None else None
    tp3 = resistance_targets[2] if len(resistance_targets) > 2 and resistance_targets[2] is not None else None

    # --- Log and Print ---
    log.info(f"[!] --- SIGNAL FOUND (SCORE: {m1_score:.1f}) --- [!]")
    log.info(f"[!] SYMBOL: {state.symbol} (Mode: {mode.upper()})")
    log.info(f"[!] Synopsis: {synopsis}")
    log.info(f"[!] CVD: ${state.cvd_slope:,.0f} | OI Spike: {state.oi_spike_pct:.1f}% (Source: {state.oi_source})")
    
    result = {
        "symbol": state.symbol, "bias": "long", "m1_score": m1_score,
        "day_change": state.price_chg_24h / 100.0 if state.price_chg_24h is not None else None,
        "taker_buy_ratio": state.fut_taker_ratio, "funding": state.funding_rate,
        "glsr_ok": (state.glsr == 'glsr_ok'), "cvd_slope": state.cvd_slope,
        "synopsis": synopsis, "entry": entry_price, "sl": sl_price,
        "tp1": tp1, "tp2": tp2, "tp3": tp3
    }
    print_signals_table([result], section=f"Live Signal ({state.symbol})")
    
    # Reset CVD slope after signal to start fresh
    state.cvd_slope = 0.0

# ----------------------------- Main Application Logic -----------------------------

async def main_async(args: List[str]) -> None:
    import argparse
    ap = argparse.ArgumentParser(description="Vertical Explosion Catcher v5.11 — Multi-Stream Engine")
    ap.add_argument("--symbols", type=str, default=None, help="Optional CSV/TXT with one futures symbol per line")
    ap.add_argument("--max-concurrency", type=int, default=20)
    ap.add_argument("--mode", type=str, default="scalp", choices=['scalp', 'hold'], help="Strategy mode: 'scalp' (30m) or 'hold' (24h)")
    ap.add_argument("--gate", type=str, default="discovery", choices=['discovery', 'strict'], help="Signal filter gate: 'discovery' (6.0) or 'strict' (9.0)")
    ns = ap.parse_args(args)

    MIN_SCORE_THRESHOLD = 6.0 if ns.gate == 'discovery' else 9.0

    log.info("--- Vertical Explosion Catcher v5.11 (Multi-Stream) | %s ---", now_iso_utc())
    log.info("--- Mode: %s | Gate: %s (Min Score: %.1f) ---", ns.mode.upper(), ns.gate.upper(), MIN_SCORE_THRESHOLD)
    log.info("Fetching exchange info and building symbol maps...")

    global fut_map, spot_map, bybit_map, bitget_map

    # This first session is *only* for the initial setup
    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT, headers=HEADERS) as setup_session:
        fut_map, spot_map, bybit_map, bitget_map = await load_all_exchange_maps(setup_session)

    if not fut_map:
        log.error("No Binance futures symbols available. Exiting.")
        return

    # --- Initialize Global State for all symbols ---
    symbols_to_scan = []
    if ns.symbols:
        sel_symbols = read_symbols_file(ns.symbols)
        symbols_to_scan = [s for s in sel_symbols if s in fut_map]
        log.info("Symbol list provided: Monitoring %d symbols.", len(symbols_to_scan))
    else:
        symbols_to_scan = sorted(list(fut_map.keys()))
        log.info("Scanning all %d Binance futures symbols...", len(symbols_to_scan))

    for symbol in symbols_to_scan:
        GLOBAL_STATE[symbol] = MarketState(symbol)
        
    # --- Launch Concurrent Tasks ---
    log.info("Launching 24/7 tasks...")
    
    # This session is persistent for the poller
    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT, headers=HEADERS) as poller_session:
        all_tasks = []
        try:
            # 1. Start the Safe Poller Task
            poller_task = asyncio.create_task(safe_poller_task(
                poller_session, spot_map, bybit_map, bitget_map, ns.mode
            ))
            all_tasks.append(poller_task)

            # 2. Start the WebSocket Stream Tasks (in chunks)
            chunk_size = 300 # 300 symbols * 3 streams/symbol = 900 streams (safe under 1024 limit)
            symbol_chunks = [symbols_to_scan[i:i + chunk_size] for i in range(0, len(symbols_to_scan), chunk_size)]
            log.info(f"Chunking {len(symbols_to_scan)} symbols into {len(symbol_chunks)} WebSocket connections...")
            
            for i, chunk in enumerate(symbol_chunks):
                ws_task = asyncio.create_task(stream_data(
                    chunk, ns.mode, MIN_SCORE_THRESHOLD, f"WS-Conn-{i+1}"
                ))
                all_tasks.append(ws_task)
                
            await asyncio.gather(*all_tasks)

        except KeyboardInterrupt:
            log.info("Shutdown signal received. Stopping tasks...")
        finally:
            for task in all_tasks:
                task.cancel()
            await asyncio.gather(*all_tasks, return_exceptions=True) # Wait for tasks to finish cancelling
            log.info("Script shut down.")


def read_symbols_file(path: str) -> List[str]:
    syms: List[str] = []
    try:
        with open(path, "r", newline="") as f:
            for line in f:
                s = normalize_symbol(line.strip())
                if s:
                    syms.append(s)
    except FileNotFoundError:
        log.error(f"Symbol file not found at {path}")
    return syms

# pretty_tables.py  — drop-in
from typing import List, Dict, Any, Optional
import math

try:
    from tabulate import tabulate
    _HAS_TABULATE = True
except Exception:
    _HAS_TABULATE = False

RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
RED   = "\033[31m"; GREEN = "\033[32m"; YELLOW = "\033[33m"
BLUE  = "\033[34m"; MAGENTA = "\033[35m"; CYAN = "\033[36m"; WHITE = "\033[37m"

try:
    import colorama
    colorama.init()
except Exception:
    pass

def _truncate(s: str, n: int = 72) -> str:
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"
def _fmt_bias(bias: str) -> str:
    b = (bias or "").lower();
    if b.startswith("long"): return f"🟢 {GREEN}long{RESET}"
    if b.startswith("short"): return f"🔴 {RED}short{RESET}"
    return f"🟡 {YELLOW}watch{RESET}"
def _fmt_pct(p: Optional[float]) -> str:
    if p is None or (isinstance(p, float) and math.isnan(p)): return f"{DIM}n/a{RESET}"
    return f"{(GREEN if p >= 0 else RED)}{p:+.2%}{RESET}"
def _fmt_tbr(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)): return f"{DIM}n/a{RESET}"
    emo = "💪" if v >= 0.65 else ("👍" if v >= 0.55 else ("😐" if v >= 0.45 else "🔻"))
    return f"{v:.2f} {emo}"
def _fmt_funding(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)): return f"{DIM}n/a{RESET}"
    emo = "🔥" if v < 0 else ("⚠️" if v > 0.0003 else "💤")
    return f"{(GREEN if v < 0 else (YELLOW if v > 0.0003 else CYAN))}{v:+.3%} {emo}{RESET}"
def _fmt_cvd(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)): return f"{DIM}n/a{RESET}"
    color = GREEN if v > 0 else (RED if v < 0 else DIM)
    v_abs = abs(v)
    if v_abs > 1_000_000: return f"{color}{v/1_000_000:+.1f}M{RESET}"
    if v_abs > 1_000: return f"{color}{v/1_000:+.1f}k{RESET}"
    return f"{color}{v:+.0f}{RESET}"
def _fmt_glsr(ok: Any) -> str:
    return "✅" if bool(ok) else "❗"
def _fmt_price(v: Optional[float], kind: str) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)) or v == 0.0: return f"{DIM}—{RESET}"
    if kind == "entry": color, emo = GREEN, "🟢"
    elif kind == "sl": color, emo = RED, "🛑"
    else: color, emo = CYAN, "🎯"
    return f"{color}{emo} {v:,.6g}{RESET}"
def _fmt_score(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)): return f"{DIM}n/a{RESET}"
    if v > 8.0: color = GREEN + BOLD
    elif v > 5.0: color = YELLOW
    else: color = RED
    return f"{color}{v:.1f}{RESET}"

def render_signals_table(rows: List[Dict[str, Any]], title: str = "", tablefmt: str = "github") -> str:
    show_levels = any(any(k in r for k in ("entry", "sl", "tp1")) for r in rows)
    headers = ["COIN", "BIAS", "SCORE", "24h Δ", "FutTaker", "Funding", "GLSR", "CVD Slope"]
    if show_levels:
        headers += ["Entry", "SL", "TP1", "TP2", "TP3"]
    headers += ["Synopsis"]
    table = []
    for r in rows:
        row = [
            r.get("symbol") or "—",
            _fmt_bias(str(r.get("bias", ""))),
            _fmt_score(r.get("m1_score")),
            _fmt_pct(r.get("day_change")),
            _fmt_tbr(r.get("taker_buy_ratio")),
            _fmt_funding(r.get("funding")),
            _fmt_glsr(r.get("glsr_ok")),
            _fmt_cvd(r.get("cvd_slope")),
        ]
        if show_levels:
            row += [
                _fmt_price(r.get("entry"), "entry"),
                _fmt_price(r.get("sl"), "sl"),
                _fmt_price(r.get("tp1"), "tp"),
                _fmt_price(r.get("tp2"), "tp"),
                _fmt_price(r.get("tp3"), "tp"),
            ]
        row.append(_truncate(r.get("synopsis") or ""))
        table.append(row)
    if _HAS_TABULATE:
        body = tabulate(table, headers=headers, tablefmt=tablefmt, stralign="center", disable_numparse=True)
    else:
        widths = [max(len(str(h)), *(len(str(r[i])) for r in table)) for i, h in enumerate(headers)]
        def sep(): return "+" + "+".join("-" * (w + 2) for w in widths) + "+"
        lines = [sep(), "| " + " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers)) + " |", sep()]
        for r in table:
            lines.append("| " + " | ".join(str(r[i]).ljust(widths[i]) for i in range(len(headers))) + " |")
        lines.append(sep())
        body = "\n".join(lines)
    title_line = (BOLD + title + RESET + "\n") if title else ""
    return title_line + body

def print_signals_table(rows: List[Dict[str, Any]], section: str = "Qualified — Long Bias", fmt: str = "github"):
    print("\n" + render_signals_table(rows, title=section, tablefmt=fmt) + "\n")

# --- MOVED TO FIX NameError ---
def main():
    try:
        asyncio.run(main_async(sys.argv[1:]))
    except KeyboardInterrupt:
        log.info("Interrupted by user. Bye.")

if __name__ == "__main__":
    main()