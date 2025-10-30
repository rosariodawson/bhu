#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Vertical Explosion Catcher v5.5 — Multi-Exchange OI
---------------------------------------------------------------------------------
Scans Binance Futures for 30-minute momentum signals.

What’s new in this patch
- NEW: Added Bybit and Bitget as fallback sources for Open Interest data.
- NEW: Script now fetches all markets from Binance, Bybit, and Bitget on startup.
- NEW: OI Fallback logic: Try Binance -> Try Bybit (if exists) -> Try Bitget (if exists).
- NEW: Log now shows which source (Binance, Bybit, Bitget) provided the OI data.
"""

import asyncio
import aiohttp
import sys
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Set

# ----------------------------- Logging helpers -----------------------------

import logging

def make_logger()->logging.Logger:
    logger = logging.getLogger("vec55")
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    h.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(h)
    return logger

log = make_logger()

def http_log(method:str, url:str, status:int):
    log.info('HTTP Request: %s %s "HTTP/1.1 %s"', method, url, status)

# ----------------------------- Endpoints -----------------------------

# Binance
SPOT_EXCHANGE_INFO = "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT"
FUT_EXCHANGE_INFO  = "https://fapi.binance.com/fapi/v1/exchangeInfo"
FUT_24HR           = "https://fapi.binance.com/fapi/v1/ticker/24hr"
SPOT_24HR          = "https://api.binance.com/api/v3/ticker/24hr"
FUT_KLINES         = "https://fapi.binance.com/fapi/v1/klines"
FUT_FUNDING        = "https://fapi.binance.com/fapi/v1/fundingRate"
FUT_TAKER_LS       = "https://fapi.binance.com/futures/data/takerlongshortRatio"
FUT_GLSR           = "https://fapi.binance.com/futures/data/globalLongShortAccountRatio"
FUT_OI_HIST        = "https://fapi.binance.com/fapi/v1/openInterestHist"

# Bybit
BYBIT_FUT_INFO   = "https://api.bybit.com/v5/market/instruments-info"
BYBIT_OI_HIST    = "https://api.bybit.com/v5/market/open-interest"

# Bitget
BITGET_FUT_INFO  = "https://api.bitget.com/api/v2/mix/market/contracts"
BITGET_OI_HIST   = "https://api.bitget.com/api/v2/mix/market/history-open-interest"

HEADERS = {
    "Accept": "application/json",
    "User-Agent": "vec55/multi-oi"
}

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)

# ----------------------------- Utils -----------------------------

def now_iso_utc()->str:
    return datetime.now(timezone.utc).isoformat()

def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default

def normalize_symbol(s: str) -> str:
    return s.strip().upper()

# ----------------------------- Data fetchers -----------------------------

async def fetch_json(session: aiohttp.ClientSession, method: str, url: str, params: Optional[Dict[str, Any]]=None) -> Tuple[Optional[Any], int]:
    try:
        async with session.request(method, url, params=params) as resp:
            status = resp.status
            http_log(method.upper(), str(resp.url), status)
            if 200 <= status < 300:
                return await resp.json(content_type=None), status
            else:
                return None, status
    except Exception as e:
        log.warning("Request error %s %s params=%s err=%s", method, url, params, e)
        return None, -1

async def load_all_exchange_maps(session: aiohttp.ClientSession) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Set[str], Set[str]]:
    """
    Returns (binance_fut_map, binance_spot_map, bybit_map, bitget_map)
    - binance_fut_map[symbol] -> { 'symbol': ..., 'pair': ..., 'status': ... }
    - bybit_map: Set[str] of symbols (e.g., "BTCUSDT")
    - bitget_map: Set[str] of symbols (e.g., "BTCUSDT_UMCBL")
    """
    # --- Binance ---
    fut_json, _ = await fetch_json(session, "GET", FUT_EXCHANGE_INFO)
    spot_json, _ = await fetch_json(session, "GET", SPOT_EXCHANGE_INFO)

    fut_map: Dict[str, Dict[str, Any]] = {}
    spot_map: Dict[str, Dict[str, Any]] = {}

    if fut_json and isinstance(fut_json, dict):
        for s in fut_json.get("symbols", []):
            sym = s.get("symbol")
            if sym:
                fut_map[sym] = {"symbol": sym, "pair": s.get("pair"), "status": s.get("status")}
    if spot_json and isinstance(spot_json, dict):
        for s in spot_json.get("symbols", []):
            sym = s.get("symbol")
            if sym:
                spot_map[sym] = {"symbol": sym, "status": s.get("status")}
    log.info("Fetched %d Binance futures and %d Binance spot symbols.", len(fut_map), len(spot_map))

    # --- Bybit ---
    bybit_map = set()
    try:
        bybit_js, _ = await fetch_json(session, "GET", BYBIT_FUT_INFO, params={"category": "linear"})
        if bybit_js and bybit_js.get("result"):
            for s in bybit_js["result"].get("list", []):
                if s.get("symbol"):
                    bybit_map.add(s["symbol"])
        log.info("Fetched %d Bybit linear futures.", len(bybit_map))
    except Exception as e:
        log.error("Failed to fetch Bybit markets: %s", e)

    # --- Bitget ---
    bitget_map = set()
    try:
        # Bitget symbols end with _UMCBL for linear futures
        bitget_js, _ = await fetch_json(session, "GET", BITGET_FUT_INFO, params={"productType": "USDT-FUTURES"})
        if bitget_js and bitget_js.get("data"):
            for s in bitget_js["data"]:
                if s.get("symbol"):
                    bitget_map.add(s["symbol"])
        log.info("Fetched %d Bitget linear futures.", len(bitget_map))
    except Exception as e:
        log.error("Failed to fetch Bitget markets: %s", e)

    return fut_map, spot_map, bybit_map, bitget_map


def futures_to_spot_symbol(fut_info: Dict[str, Any], spot_map: Dict[str, Dict[str, Any]]) -> Optional[str]:
    """
    Maps a futures symbol to a Spot symbol using the `pair` field.
    Example: 1000SHIBUSDT (Futures) -> pair:'SHIBUSDT' -> Spot:'SHIBUSDT' (exists) -> return 'SHIBUSDT'
    """
    spot_candidate = fut_info.get("pair")
    if not spot_candidate:
        return None
    if spot_candidate in spot_map:
        return spot_candidate
    return None

async def get_price_change_percent_24h(session: aiohttp.ClientSession, fut_symbol: str) -> Optional[float]:
    """Fetches the 24h price change for table context."""
    js, _ = await fetch_json(session, "GET", FUT_24HR, params={"symbol": fut_symbol})
    if js:
        return safe_float(js.get("priceChangePercent"), None)
    return None

async def get_price_change_30m(session: aiohttp.ClientSession, fut_symbol: str) -> Optional[float]:
    """
    Calculates the 30-minute price change using 6 x 5m klines.
    """
    # [ [open_time, open, high, low, close, volume, ..., ..., ..., ..., ..., ...] ]
    js, _ = await fetch_json(session, "GET", FUT_KLINES, params={"symbol": fut_symbol, "interval": "5m", "limit": 6})
    if not js or not isinstance(js, list) or len(js) < 6:
        return None
    
    try:
        start_price = safe_float(js[0][1]) # Open of the first candle
        end_price = safe_float(js[5][4])   # Close of the sixth candle
        
        if start_price == 0:
            return None
        
        return ((end_price / start_price) - 1.0) * 100.0
    except Exception as e:
        log.warning("%s: Error calculating 30m price change: %s", fut_symbol, e)
        return None


async def get_spot_taker_ratio(session: aiohttp.ClientSession, spot_symbol: str) -> Optional[float]:
    """Uses SPOT 24hr stats -> takerBuyBaseAssetVolume / volume"""
    js, _ = await fetch_json(session, "GET", SPOT_24HR, params={"symbol": spot_symbol})
    if not js:
        return None
    taker_buy = safe_float(js.get("takerBuyBaseAssetVolume"))
    vol = max(safe_float(js.get("volume")), 1e-12)
    return max(0.0, min(1.0, taker_buy / vol))

async def get_futures_taker_ratio(session: aiohttp.ClientSession, fut_symbol: str) -> Optional[float]:
    """
    Uses taker long/short ratio (latest) and normalize buySellRatio -> [0..1]
    buySellRatio = taker buy volume / taker sell volume
    -> normalized = b / (b + s) = r / (1 + r)
    """
    js, status = await fetch_json(session, "GET", FUT_TAKER_LS, params={"symbol": fut_symbol, "period": "5m", "limit": 1})
    if not js or not isinstance(js, list) or not js:
        return None
    r = safe_float(js[-1].get("buySellRatio"))
    if r <= 0:
        return 0.0
    return r / (1.0 + r)

async def get_avg_funding_raw(session: aiohttp.ClientSession, fut_symbol: str, limit: int = 21) -> Optional[float]:
    """Average recent funding rate (raw decimal)."""
    js, _ = await fetch_json(session, "GET", FUT_FUNDING, params={"symbol": fut_symbol, "limit": limit})
    if not js or not isinstance(js, list) or not js:
        return None
    vals = [safe_float(x.get("fundingRate")) for x in js if "fundingRate" in x]
    if not vals:
        return None
    # Return raw average decimal (e.g., -0.00019)
    return sum(vals)/len(vals)

async def get_glsr_class(session: aiohttp.ClientSession, fut_symbol: str) -> str:
    """Classify global long/short account ratio as 'glsr_ok' or 'glsr_weak' using last value."""
    js, _ = await fetch_json(session, "GET", FUT_GLSR, params={"symbol": fut_symbol, "period": "5m", "limit": 50})
    if not js or not isinstance(js, list) or not js:
        return "glsr_weak"
    last = js[-1]
    ratio = safe_float(last.get("longShortRatio"), 0.5)
    # Simple band: neutral band [0.48, 0.52] -> weak, otherwise ok
    if 0.48 <= ratio <= 0.52:
        return "glsr_weak"
    return "glsr_ok"

async def get_oi_spike_percent_binance(session: aiohttp.ClientSession, fut_symbol: str) -> float:
    """
    Calculates the 40-minute OI spike from BINANCE.
    Compares the last 10 mins (2x 5m) vs the previous 30 mins (6x 5m).
    """
    js, _ = await fetch_json(session, "GET", FUT_OI_HIST, params={"symbol": fut_symbol, "period": "5m", "limit": 8})
    if not js or not isinstance(js, list) or len(js) < 8:
        return 0.0 
    try:
        baseline_data = [safe_float(d.get("sumOpenInterestValue")) for d in js[0:6]]
        recent_data = [safe_float(d.get("sumOpenInterestValue")) for d in js[6:8]]
        if not baseline_data or not recent_data: return 0.0
        baseline_avg = sum(baseline_data) / len(baseline_data)
        recent_avg = sum(recent_data) / len(recent_data)
        if baseline_avg == 0: return 0.0
        spike_pct = ((recent_avg / baseline_avg) - 1.0) * 100.0
        return max(0.0, spike_pct)
    except Exception as e:
        log.warning("%s (Binance): Error calculating OI spike: %s", fut_symbol, e)
        return 0.0

async def get_oi_spike_percent_bybit(session: aiohttp.ClientSession, fut_symbol: str) -> float:
    """
    Calculates the 40-minute OI spike from BYBIT.
    Compares the last 10 mins (2x 5m) vs the previous 30 mins (6x 5m).
    """
    params = {"category": "linear", "symbol": fut_symbol, "intervalTime": "5min", "limit": 8}
    js, _ = await fetch_json(session, "GET", BYBIT_OI_HIST, params=params)
    if not js or not js.get("result") or not js["result"].get("list"):
        return 0.0
    
    data_list = js["result"]["list"]
    if len(data_list) < 8:
        return 0.0
        
    try:
        # Bybit returns in reverse order (newest first), so we reverse it
        data_list.reverse()
        baseline_data = [safe_float(d.get("openInterest")) for d in data_list[0:6]]
        recent_data = [safe_float(d.get("openInterest")) for d in data_list[6:8]]
        if not baseline_data or not recent_data: return 0.0
        baseline_avg = sum(baseline_data) / len(baseline_data)
        recent_avg = sum(recent_data) / len(recent_data)
        if baseline_avg == 0: return 0.0
        spike_pct = ((recent_avg / baseline_avg) - 1.0) * 100.0
        return max(0.0, spike_pct)
    except Exception as e:
        log.warning("%s (Bybit): Error calculating OI spike: %s", fut_symbol, e)
        return 0.0

async def get_oi_spike_percent_bitget(session: aiohttp.ClientSession, fut_symbol: str) -> float:
    """
    Calculates the 40-minute OI spike from BITGET.
    Compares the last 10 mins (2x 5m) vs the previous 30 mins (6x 5m).
    Note: Bitget symbols are different, e.g., "BTCUSDT_UMCBL"
    """
    # Bitget symbols on Binance (BTCUSDT) map to (BTCUSDT_UMCBL) on Bitget
    bitget_symbol = f"{fut_symbol}_UMCBL"
    
    params = {"symbol": bitget_symbol, "interval": "5m", "limit": 8}
    js, _ = await fetch_json(session, "GET", BITGET_OI_HIST, params=params)
    if not js or not js.get("data"):
        return 0.0

    data_list = js["data"]
    if len(data_list) < 8:
        return 0.0

    try:
        # Bitget also returns in reverse order (newest first)
        data_list.reverse()
        baseline_data = [safe_float(d.get("openInterest")) for d in data_list[0:6]]
        recent_data = [safe_float(d.get("openInterest")) for d in data_list[6:8]]
        if not baseline_data or not recent_data: return 0.0
        baseline_avg = sum(baseline_data) / len(baseline_data)
        recent_avg = sum(recent_data) / len(recent_data)
        if baseline_avg == 0: return 0.0
        spike_pct = ((recent_avg / baseline_avg) - 1.0) * 100.0
        return max(0.0, spike_pct)
    except Exception as e:
        log.warning("%s (Bitget): Error calculating OI spike: %s", fut_symbol, e)
        return 0.0

# ----------------------------- Filters (edit thresholds as you like) -----------------------------

def evaluate_m1_score(
    price_chg_30m: Optional[float],
    spot_taker: Optional[float],
    fut_taker: Optional[float],
    avg_funding_raw: Optional[float],
    oi_spike_pct: float
) -> float:
    """
    M1: 12-point signal scoring system.
    Returns a score, not True/False.
    """
    score = 0.0
    synopsis_notes = []

    # 1. Price Change (30-Min) (Max: 4 points)
    if price_chg_30m is not None:
        if price_chg_30m > 0.5:
            score += 1.0
            synopsis_notes.append("Δ30m>0.5%")
        if price_chg_30m > 2.0:
            score += 2.0 # Extra points for strong change
            synopsis_notes.append("Δ30m>2%!!")
        if price_chg_30m < 10.0:
            score += 1.0 # Not *too* high (not a 10% wick)
        
    # 2. Taker Ratios (Max: 4 points)
    is_spot_bullish = (spot_taker is not None and spot_taker > 0.52)
    is_fut_bullish = (fut_taker is not None and fut_taker > 0.52)
    
    if is_fut_bullish:
        score += 1.0
        synopsis_notes.append("FutTkr")
    if is_spot_bullish:
        score += 1.0
        synopsis_notes.append("SpotTkr")
    if is_fut_bullish and is_spot_bullish:
        score += 2.0 # Taker Alignment is a strong signal
        synopsis_notes.append("TkrAlign")

    # 3. Funding Rate (Max: 2 points)
    if avg_funding_raw is not None:
        if avg_funding_raw < 0.0001: # Neutral or negative
            score += 1.0
            synopsis_notes.append("FundOK")
        if avg_funding_raw < -0.0001: # Clearly negative (squeeze fuel)
            score += 1.0
            synopsis_notes.append("FundNeg🔥")
            
    # 4. Open Interest Spike (Max: 2 points)
    if oi_spike_pct > 15.0:
        score += 1.0
        synopsis_notes.append("OI>15%")
    if oi_spike_pct > 30.0:
        score += 1.0 # Extra point for strong spike
        synopsis_notes.append("OI>30%!")

    # Attach synopsis notes to the score
    return score, " | ".join(synopsis_notes)


# ----------------------------- Scanner -----------------------------

async def scan_symbol(
    session: aiohttp.ClientSession, 
    fut_info: Dict[str, Any], 
    spot_map: Dict[str, Dict[str, Any]],
    bybit_map: Set[str],
    bitget_map: Set[str]
) -> Optional[Dict[str, Any]]:
    
    # --- !! SET YOUR MINIMUM SCORE THRESHOLD HERE !! ---
    MIN_SCORE_THRESHOLD = 6.0
    # ---------------------------------------------------

    fut_symbol = fut_info["symbol"]
    spot_symbol = futures_to_spot_symbol(fut_info, spot_map)

    # Gather data
    price_chg_30m = await get_price_change_30m(session, fut_symbol)
    price_chg_24h = await get_price_change_percent_24h(session, fut_symbol)
    
    spot_taker = None
    if spot_symbol:
        spot_taker = await get_spot_taker_ratio(session, spot_symbol)

    fut_taker = await get_futures_taker_ratio(session, fut_symbol)
    avg_funding_raw = await get_avg_funding_raw(session, fut_symbol)
    
    # --- New OI Fallback Logic ---
    oi_spike_pct = 0.0
    oi_source = "None"

    # 1. Try Binance
    oi_spike_pct = await get_oi_spike_percent_binance(session, fut_symbol)
    if oi_spike_pct > 0:
        oi_source = "Binance"
    
    # 2. Try Bybit (if Binance failed and symbol exists)
    if oi_source == "None" and fut_symbol in bybit_map:
        oi_spike_pct = await get_oi_spike_percent_bybit(session, fut_symbol)
        if oi_spike_pct > 0:
            oi_source = "Bybit"

    # 3. Try Bitget (if both failed and symbol exists)
    # Bitget symbols are different, e.g., "BTCUSDT_UMCBL"
    bitget_symbol = f"{fut_symbol}_UMCBL"
    if oi_source == "None" and bitget_symbol in bitget_map:
        oi_spike_pct = await get_oi_spike_percent_bitget(session, fut_symbol)
        if oi_spike_pct > 0:
            oi_source = "Bitget"
    # --- End of New Logic ---

    # 1. Run M1 Scoring
    m1_score, synopsis = evaluate_m1_score(price_chg_30m, spot_taker, fut_taker, avg_funding_raw, oi_spike_pct)

    if m1_score < MIN_SCORE_THRESHOLD:
        return None # Failed M1 score threshold

    # 2. M1 Passed, get M2 data
    glsr = await get_glsr_class(session, fut_symbol)
    glsr_ok = (glsr == 'glsr_ok')

    # Log the pass
    log.info("[+] SCORE %.1f: %s (PriceChg_30m: %s%%, FutTaker: %s, Fund: %s, OI Spike: %.1f%% [Source: %s])",
             m1_score,
             fut_symbol,
             f"{price_chg_30m:.2f}" if price_chg_30m is not None else "—",
             f"{fut_taker:.2f}" if fut_taker is not None else "—",
             f"{avg_funding_raw:+.5f}" if avg_funding_raw is not None else "—",
             oi_spike_pct,
             oi_source # Added OI source to log
             )
    log.info("[M2] %s -> %s | Synopsis: %s", fut_symbol, glsr, synopsis)

    # 3. Build result dict for the table
    result = {
        "symbol": fut_symbol,
        "bias": "long",
        "m1_score": m1_score,
        "day_change": price_chg_24h / 100.0 if price_chg_24h is not None else None, # Use 24h for table
        "taker_buy_ratio": fut_taker if fut_taker is not None else spot_taker, # Use FutTaker as primary
        "funding": avg_funding_raw,
        "glsr_ok": glsr_ok,
        "synopsis": synopsis
    }
    return result

async def producer_consumer(
    fut_map: Dict[str, Dict[str, Any]], 
    spot_map: Dict[str, Dict[str, Any]], 
    symbols: Optional[List[str]], 
    max_concurrency: int = 20,
    bybit_map: Set[str] = set(),
    bitget_map: Set[str] = set()
) -> List[Dict[str, Any]]:
    
    sem = asyncio.Semaphore(max_concurrency)
    conn = aiohttp.TCPConnector(limit=128, ssl=False)
    
    # This list will store our results
    results: List[Dict[str, Any]] = []

    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT, headers=HEADERS, connector=conn) as session:

        async def bounded(fut_info: Dict[str, Any]):
            async with sem:
                try:
                    # Pass the new maps to scan_symbol
                    result = await scan_symbol(session, fut_info, spot_map, bybit_map, bitget_map)
                    if result:
                        results.append(result)
                except Exception as e:
                    log.warning("Scan error for %s: %s", fut_info.get("symbol"), e)

        tasks = []
        for sym, info in fut_map.items():
            if symbols and sym not in symbols:
                continue
            tasks.append(asyncio.create_task(bounded(info)))

        # simple pacing to avoid spikes
        BATCH = 100
        for i in range(0, len(tasks), BATCH):
            batch = tasks[i:i+BATCH]
            await asyncio.gather(*batch)
            if i + BATCH < len(tasks):
                await asyncio.sleep(0.5)

    # Return the collected list of results
    return results

def read_symbols_file(path: str) -> List[str]:
    syms: List[str] = []
    with open(path, "r", newline="") as f:
        for line in f:
            s = normalize_symbol(line.strip())
            if s:
                syms.append(s)
    return syms

async def main_async(args: List[str]) -> None:
    import argparse
    ap = argparse.ArgumentParser(description="Vertical Explosion Catcher v5.5 — Multi-Exchange OI")
    ap.add_argument("--symbols", type=str, default=None, help="Optional CSV/TXT with one futures symbol per line")
    ap.add_argument("--max-concurrency", type=int, default=20)
    ns = ap.parse_args(args)

    log.info("--- Vertical Explosion Catcher v5.5 (Multi-OI) | %s ---", now_iso_utc())
    log.info("Fetching exchange info and building Futures↔Spot map...")

    conn = aiohttp.TCPConnector(limit=128, ssl=False)
    async with aiohttp.ClientSession(timeout=HTTP_TIMEOUT, headers=HEADERS, connector=conn) as session:
        fut_map, spot_map, bybit_map, bitget_map = await load_all_exchange_maps(session)

    if not fut_map:
        log.error("No Binance futures symbols available. Network or endpoint issue.")
        return

    # universe
    fut_symbols = sorted(fut_map.keys())
    sel_symbols: Optional[List[str]] = None
    if ns.symbols:
        sel_symbols = read_symbols_file(ns.symbols)
        log.info("Symbol list provided: %d symbols (subset of %d)", len(sel_symbols), len(fut_symbols))
    else:
        log.info("Scanning %d Binance futures symbols...", len(fut_symbols))

    # Run the scanner and get the list of results
    all_results = await producer_consumer(
        fut_map, spot_map, sel_symbols, ns.max_concurrency, bybit_map, bitget_map
    )
    
    log.info("--- Scan complete. Found %d signals. ---", len(all_results))

    # Sort and print the table
    if all_results:
        # Sort by M1 Score (descending)
        sorted_results = sorted(all_results, key=lambda r: r.get("m1_score") or -1, reverse=True)
        
        # Use the imported function to print the *real* results
        print_signals_table(sorted_results, section="Live Scan Signals (Ranked by Score)", fmt="github")
    else:
        log.info("No symbols matched your filters.")


# pretty_tables.py  — drop-in
from typing import List, Dict, Any, Optional
import math

# Optional: best-looking tables if available
try:
    from tabulate import tabulate
    _HAS_TABULATE = True
except Exception:
    _HAS_TABULATE = False

# ANSI colors (work on macOS/Linux; Colorama makes them work on Windows)
RESET = "\033[0m"; BOLD = "\033[1m"; DIM = "\033[2m"
RED   = "\033[31m"; GREEN = "\033[32m"; YELLOW = "\033[33m"
BLUE  = "\033[34m"; MAGENTA = "\033[35m"; CYAN = "\033[36m"; WHITE = "\033[37m"

# Try to enable Colorama on Windows (safe no-op elsewhere)
try:
    import colorama
    colorama.init()
except Exception:
    pass

def _truncate(s: str, n: int = 72) -> str:
    s = "" if s is None else str(s)
    return s if len(s) <= n else s[: max(0, n - 1)] + "…"

def _fmt_bias(bias: str) -> str:
    b = (bias or "").lower()
    if b.startswith("long") or b in {"bull", "bullish", "🟢"}:
        return f"🟢 {GREEN}long{RESET}"
    if b.startswith("short") or b in {"bear","bearish", "🔴"}:
        return f"🔴 {RED}short{RESET}"
    return f"🟡 {YELLOW}watch{RESET}"

def _fmt_pct(p: Optional[float]) -> str:
    if p is None or (isinstance(p, float) and math.isnan(p)):
        return f"{DIM}n/a{RESET}"
    return f"{(GREEN if p >= 0 else RED)}{p:+.2%}{RESET}"

def _fmt_tbr(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return f"{DIM}n/a{RESET}"
    emo = "💪" if v >= 0.65 else ("👍" if v >= 0.55 else ("😐" if v >= 0.45 else "🔻"))
    return f"{v:.2f} {emo}"

def _fmt_funding(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return f"{DIM}n/a{RESET}"
    # v is decimal (e.g., -0.00019 == -0.019%)
    emo = "🔥" if v < 0 else ("⚠️" if v > 0.0003 else "💤")
    return f"{(GREEN if v < 0 else (YELLOW if v > 0.0003 else CYAN))}{v:+.3%} {emo}{RESET}"

def _fmt_glsr(ok: Any) -> str:
    return "✅" if bool(ok) else "❗"

def _fmt_price(v: Optional[float], kind: str) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return f"{DIM}—{RESET}"
    if kind == "entry":
        color, emo = GREEN, "🟢"
    elif kind == "sl":
        color, emo = RED, "🛑"
    else:
        color, emo = CYAN, "🎯"
    return f"{color}{emo} {v:,.6g}{RESET}"

def _fmt_score(v: Optional[float]) -> str:
    if v is None or (isinstance(v, float) and math.isnan(v)):
        return f"{DIM}n/a{RESET}"
    if v > 8.0:
        color = GREEN + BOLD
    elif v > 5.0:
        color = YELLOW
    else:
        color = RED
    return f"{color}{v:.1f}{RESET}"

def render_signals_table(rows: List[Dict[str, Any]], title: str = "", tablefmt: str = "github") -> str:
    """
    Expected keys per row (case-insensitive fallbacks handled):
      symbol/coin, bias, m1_score, day_change (float), taker_buy_ratio (float),
      funding (float), glsr_ok (bool), synopsis (str),
      entry, sl, tp1, tp2, tp3 (floats, optional)
    """
    # Decide columns (levels shown if any exist)
    show_levels = any(any(k in r for k in ("entry", "sl", "tp1", "tp2", "tp3")) for r in rows)
    headers = ["COIN", "BIAS", "SCORE", "24h Δ", "FutTaker", "Funding", "GLSR"]
    if show_levels:
        headers += ["Entry", "SL", "TP1", "TP2", "TP3"]
    headers += ["Synopsis"]

    table = []
    for r in rows:
        coin = r.get("symbol") or r.get("COIN") or r.get("coin") or "—"
        bias = _fmt_bias(str(r.get("bias", "")))
        score = _fmt_score(r.get("m1_score"))
        delta = _fmt_pct(r.get("day_change") if "day_change" in r else r.get("24hDelta"))
        tbr = _fmt_tbr(r.get("taker_buy_ratio") if "taker_buy_ratio" in r else r.get("FutTaker"))
        funding = _fmt_funding(r.get("funding") if "funding" in r else r.get("Funding"))
        glsr = _fmt_glsr(r.get("glsr_ok") if "glsr_ok" in r else r.get("GLSR"))
        syn = _truncate(r.get("synopsis") or r.get("SYNOPSIS") or "")
        
        row = [coin, bias, score, delta, tbr, funding, glsr]
        
        if show_levels:
            row += [
                _fmt_price(r.get("entry"), "entry"),
                _fmt_price(r.get("sl") or r.get("stop"), "sl"),
                _fmt_price(r.get("tp1"), "tp"),
                _fmt_price(r.get("tp2"), "tp"),
                _fmt_price(r.get("tp3"), "tp"),
            ]
        row.append(syn)
        table.append(row)

    if _HAS_TABULATE:
        # tabulate keeps ANSI codes visible and supports lots of formats
        body = tabulate(table, headers=headers, tablefmt=tablefmt, stralign="center", disable_numparse=True)
    else:
        # minimal, clean fallback table
        widths = [max(len(str(h)), *(len(str(r[i])) for r in table)) for i, h in enumerate(headers)]
        def sep():
            return "+" + "+".join("-" * (w + 2) for w in widths) + "+"
        lines = [sep(), "| " + " | ".join(h.ljust(widths[i]) for i, h in enumerate(headers)) + " |", sep()]
        for r in table:
            lines.append("| " + " | ".join(str(r[i]).ljust(widths[i]) for i in range(len(headers))) + " |")
        lines.append(sep())
        body = "\n".join(lines)

    title_line = (BOLD + title + RESET + "\n") if title else ""
    return title_line + body

def print_signals_table(rows: List[Dict[str, Any]], section: str = "Qualified — Long Bias", fmt: str = "github"):
    print(render_signals_table(rows, title=section, tablefmt=fmt))

# --- MOVED TO FIX NameError ---
def main():
    try:
        asyncio.run(main_async(sys.argv[1:]))
    except KeyboardInterrupt:
        log.info("Interrupted by user. Bye.")

if __name__ == "__main__":
    main()