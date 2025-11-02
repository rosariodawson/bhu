#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Vertical Explosion Catcher v9.0 — Tuned CVD Scoring
---------------------------------------------------------------------------------
This single script performs two tasks at once using asyncio:

1.  BACKGROUND ENGINE: Connects to Bybit's WebSocket, listens to all trades,
    and calculates a 1-hour rolling CVD *in memory*.
2.  FOREGROUND SCANNER: Runs the main screener logic, fetches API data,
    and reads the CVD data from memory for scoring.

*** UPGRADE v9.0 ***
- CRITICAL FIX: CVD Engine now calculates CVD in USDT (Price * Size)
  instead of base currency (Size). This makes the value accurate.
- TUNE: Scoring logic is no longer a static $10k threshold.
- TUNE: Score is now dynamic, tiered, and relative to the coin's
  own 24-hour average volume.
"""

import asyncio
import aiohttp
import sys
import time
import math
import orjson
import sqlite3  # Still needed for 'liquidations.db'
import websockets
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Tuple, Set
from collections import defaultdict

# --- TABLE AND COLOR IMPORTS ---
import colorama
from tabulate import tabulate

# -------------------------------

# --- Configuration ---
LIQUIDATIONS_DB_FILE = "liquidations.db"
COINGLASS_V3_BASE_URL = "https://open-api-v3.coinglass.com/api"

# --- CVD Engine Config ---
BYBIT_V5_STREAM = "wss://stream.bybit.com/v5/public/linear"
BYBIT_V5_API = "https://api.bybit.com/v5/market/instruments-info"
CVD_WINDOW_SECONDS = 3600  # 1 hour (3600 seconds)

# --- Global In-Memory CVD Storage ---
# This dictionary holds all real-time CVD data.
# Format: { "symbol": {"cvd": 1234.5, "trades": [(ts, vol_usdt), ...]} }
cvd_state_manager: Dict[str, Dict[str, Any]] = {}
cvd_lock = asyncio.Lock()  # Lock to prevent data corruption


# ----------------------------- Logging helpers -----------------------------
def make_logger() -> logging.Logger:
    logger = logging.getLogger("vec9")
    logger.setLevel(logging.INFO)
    h = logging.StreamHandler(sys.stdout)
    fmt = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    h.setFormatter(fmt)
    if not logger.handlers:
        logger.addHandler(h)
    return logger


log = make_logger()


def http_log(method: str, url: str, status: int):
    log.info('HTTP Request: %s %s "HTTP/1.1 %s"', method, url, status)


# ----------------------------- Binance Endpoints & Config -----------------------------
SPOT_EXCHANGE_INFO = "exchangeInfo?permissions=SPOT"
FUT_EXCHANGE_INFO = "exchangeInfo"
FUT_24HR = "ticker/24hr"
FUT_KLINES = "klines"
SPOT_24HR = "ticker/24hr"
FUT_FUNDING = "fundingRate"
FUT_GLSR = "globalLongShortAccountRatio"
FUT_OI_HIST = "openInterestHist"

HEADERS = {"Accept": "application/json", "User-Agent": "vec9/tuned-engine"}

HTTP_TIMEOUT = aiohttp.ClientTimeout(total=20)
BINANCE_SAFE_LIMIT_PER_MINUTE = 1800.0
BINANCE_FUTURES_WEIGHTS = {
    "exchangeInfo": 20.0,
    "ticker/24hr": 1.0,
    "klines": 1.0,
    "fundingRate": 1.0,
    "openInterestHist": 1.0,
    "globalLongShortAccountRatio": 1.0,
}


# ----------------------------- Utils & Rate Limiter -----------------------------
def now_iso_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(str(x))
    except Exception:
        return default


def normalize_symbol(s: str) -> str:
    return s.strip().upper()


class AsyncWeightedTokenBucket:
    def __init__(self, capacity: float, interval_s: float = 60.0, loop=None):
        self.capacity = capacity
        self.rate = capacity / interval_s
        self._tokens = capacity
        self.last_refill = time.monotonic()
        try:
            self.loop = loop or asyncio.get_event_loop()
        except RuntimeError:
            self.loop = None
        self.lock = asyncio.Lock()
        log.info(
            f"Initialized Weighted Token Bucket: Capacity {self.capacity:.0f} / {interval_s}s ({self.rate:.2f} tokens/s)"
        )

    async def acquire(self, weight: float = 1.0):
        async with self.lock:
            await self._refill()
            while self._tokens < weight:
                time_to_wait = (weight - self._tokens) / self.rate
                log.warning(
                    f"Throttling request (W:{weight:.0f}). Remaining: {self._tokens:.2f}. Waiting {time_to_wait:.3f}s to refill."
                )
                await asyncio.sleep(time_to_wait + 0.05)
                await self._refill()
            self._tokens -= weight

    async def _refill(self):
        now = time.monotonic()
        elapsed = now - self.last_refill
        new_tokens = elapsed * self.rate
        if new_tokens > 0:
            self._tokens = min(self.capacity, self._tokens + new_tokens)
            self.last_refill = now


class BinanceManager:
    def __init__(self):
        self.limiter = AsyncWeightedTokenBucket(BINANCE_SAFE_LIMIT_PER_MINUTE)
        self.base_url_fut = "https://fapi.binance.com/fapi/v1"
        self.base_url_spot = "https://api.binance.com/api/v3"
        self.base_url_data = "https://fapi.binance.com/futures/data"
        self.weights = BINANCE_FUTURES_WEIGHTS

    def _get_weight(self, endpoint: str) -> float:
        path = endpoint.strip("/").split("?")[0]
        if "openInterestHist" in path:
            return self.weights["openInterestHist"]
        if "globalLongShortAccountRatio" in path:
            return self.weights["globalLongShortAccountRatio"]
        if "klines" in path:
            return self.weights["klines"]
        return self.weights.get(path, 1.0)

    def _get_url(self, endpoint: str, base_url_type: str) -> str:
        if base_url_type == "futures":
            return self.base_url_fut
        if base_url_type == "spot":
            return self.base_url_spot
        if base_url_type == "data":
            return self.base_url_data
        return endpoint

    async def _fetch_binance(
        self,
        session: aiohttp.ClientSession,
        method: str,
        endpoint: str,
        params: Optional[Dict] = None,
        base_url_type: str = "futures",
    ) -> Tuple[Optional[Any], int]:
        weight = self._get_weight(endpoint)
        base_url = self._get_url(endpoint, base_url_type)
        full_url = f"{base_url}/{endpoint}"
        await self.limiter.acquire(weight)
        try:
            async with session.request(
                method, full_url, params=params, timeout=10
            ) as resp:
                status = resp.status
                http_log(method.upper(), str(resp.url), status)
                if status == 429 or status == 418:
                    resp.raise_for_status()
                resp.raise_for_status()
                return await resp.json(content_type=None), status
        except Exception as e:
            raise


BINANCE_MGR = BinanceManager()

# ----------------------------- CVD Background Engine Logic -----------------------------


async def get_bybit_symbols() -> Set[str]:
    """Fetches all active Bybit linear (USDT) symbols for the CVD engine."""
    log.info("CVD Engine: Fetching all Bybit symbols...")
    symbols = set()
    params = {"category": "linear", "limit": 1000}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(BYBIT_V5_API, params=params) as resp:
                resp.raise_for_status()
                data = await resp.json()
                if data.get("retCode") == 0:
                    for item in data.get("result", {}).get("list", []):
                        if item.get("status") == "Trading":
                            symbols.add(item["symbol"])
                    log.info(
                        f"CVD Engine: Fetched {len(symbols)} active Bybit linear symbols."
                    )
                    return symbols
                else:
                    log.error(f"CVD Engine: Bybit API error: {data.get('retMsg')}")
                    return symbols
    except Exception as e:
        log.error(f"CVD Engine: Failed to fetch Bybit symbols: {e}")
        return symbols


async def update_cvd_in_memory(
    symbol: str, side: str, price: float, volume: float, timestamp: int
):
    """
    Updates the in-memory CVD for a symbol.

    *** v9.0 FIX ***
    Calculates CVD using USDT value (price * volume) instead of just volume.
    """
    global cvd_state_manager, cvd_lock

    # Calculate the USDT value of the trade
    usdt_volume = volume * price

    async with cvd_lock:
        # --- 1. Initialize if new symbol ---
        if symbol not in cvd_state_manager:
            cvd_state_manager[symbol] = {"cvd": 0.0, "trades": []}

        # --- 2. Add new trade ---
        signed_usdt_volume = usdt_volume if side == "Buy" else -usdt_volume
        cvd_state_manager[symbol]["cvd"] += signed_usdt_volume
        cvd_state_manager[symbol]["trades"].append((timestamp, signed_usdt_volume))

        # --- 3. Prune old trades ---
        cutoff_time = timestamp - (CVD_WINDOW_SECONDS * 1000)  # Convert window to ms

        trades_to_remove = 0
        for ts, vol_usdt in cvd_state_manager[symbol]["trades"]:
            if ts < cutoff_time:
                cvd_state_manager[symbol][
                    "cvd"
                ] -= vol_usdt  # Subtract the expired trade's USDT volume
                trades_to_remove += 1
            else:
                break  # Timestamps are sorted

        if trades_to_remove > 0:
            cvd_state_manager[symbol]["trades"] = cvd_state_manager[symbol]["trades"][
                trades_to_remove:
            ]


async def background_cvd_engine():
    """
    The main background task. Connects to Bybit, subscribes,
    and updates the global cvd_state_manager dictionary.
    """
    log.info("CVD Engine: Task starting...")
    symbols = await get_bybit_symbols()
    if not symbols:
        log.error("CVD Engine: No symbols fetched. Task cannot start.")
        return

    sub_args = [f"publicTrade.{s}" for s in symbols]

    while True:  # Keep-alive loop
        try:
            log.info(
                f"CVD Engine: Connecting to Bybit WebSocket at {BYBIT_V5_STREAM}..."
            )
            async with websockets.connect(BYBIT_V5_STREAM) as websocket:

                # 1. Send subscription
                await websocket.send(
                    orjson.dumps({"op": "subscribe", "args": sub_args})
                )

                # 2. Start the PING (heartbeat) task
                async def heartbeat(ws):
                    while True:
                        await asyncio.sleep(20)
                        try:
                            await ws.send(orjson.dumps({"op": "ping"}))
                        except websockets.exceptions.ConnectionClosed:
                            log.warning(
                                "CVD Engine: Heartbeat failed, connection closed."
                            )
                            break

                heartbeat_task = asyncio.create_task(heartbeat(websocket))
                log.info(
                    f"CVD Engine: Successfully subscribed to {len(sub_args)} trade streams."
                )

                # 3. Main message listener loop
                async for message in websocket:
                    data = orjson.loads(message)

                    if data.get("op") == "subscribe" and not data.get("success", False):
                        log.error(
                            f"CVD Engine: Subscription failed: {data.get('ret_msg')}"
                        )

                    elif data.get("topic", "").startswith("publicTrade"):
                        for trade in data.get("data", []):
                            try:
                                # *** v9.0 FIX: Pass price and volume ***
                                await update_cvd_in_memory(
                                    symbol=trade["s"],
                                    side=trade["S"],  # Taker side (Buy/Sell)
                                    price=float(trade["p"]),  # Trade price
                                    volume=float(
                                        trade["v"]
                                    ),  # Trade size in base currency
                                    timestamp=int(trade["T"]),  # Trade timestamp (ms)
                                )
                            except Exception as e:
                                log.warning(
                                    f"CVD Engine: Error processing trade: {e} | Data: {trade}"
                                )

        except websockets.exceptions.ConnectionClosed as e:
            log.warning(
                f"CVD Engine: WebSocket closed, reconnecting in 5s... Error: {e}"
            )
        except Exception as e:
            log.error(f"CVD Engine: Main listener error: {e}. Reconnecting in 10s...")
        finally:
            if "heartbeat_task" in locals() and not heartbeat_task.done():
                heartbeat_task.cancel()
            await asyncio.sleep(5)  # Wait before reconnecting


# ----------------------------- Screener Data Fetchers -----------------------------


async def fetch_json(
    session: aiohttp.ClientSession,
    method: str,
    url: str,
    params: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Any], int]:
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


async def load_all_exchange_maps(
    session: aiohttp.ClientSession,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]], Set[str], Set[str]]:
    fut_task = BINANCE_MGR._fetch_binance(
        session, "GET", FUT_EXCHANGE_INFO, base_url_type="futures"
    )
    spot_task = BINANCE_MGR._fetch_binance(
        session, "GET", SPOT_EXCHANGE_INFO, base_url_type="spot"
    )
    (fut_json, _), (spot_json, _) = await asyncio.gather(fut_task, spot_task)
    fut_map, spot_map = {}, {}
    if fut_json:
        for s in fut_json.get("symbols", []):
            fut_map[s.get("symbol")] = {
                "symbol": s.get("symbol"),
                "pair": s.get("pair"),
                "status": s.get("status"),
            }
    if spot_json:
        for s in spot_json.get("symbols", []):
            spot_map[s.get("symbol")] = {
                "symbol": s.get("symbol"),
                "status": s.get("status"),
            }
    log.info(
        "Screener: Fetched %d Binance futures and %d Binance spot symbols.",
        len(fut_map),
        len(spot_map),
    )
    bybit_map, bitget_map = set(), set()
    return fut_map, spot_map, bybit_map, bitget_map


# --- v9.0 TUNE: Renamed function to get volume as well ---
async def get_price_change_and_volume(
    session: aiohttp.ClientSession, fut_symbol: str
) -> Tuple[Optional[float], Optional[float]]:
    """Fetches 24h Price Change % and 24h Quote Volume."""
    js, _ = await BINANCE_MGR._fetch_binance(
        session, "GET", FUT_24HR, params={"symbol": fut_symbol}, base_url_type="futures"
    )
    if js:
        price_change = safe_float(js.get("priceChangePercent"), None)
        quote_volume = safe_float(js.get("quoteVolume"), None)
        return (price_change, quote_volume)
    return (None, None)


async def get_spot_taker_ratio(
    session: aiohttp.ClientSession, spot_symbol: str
) -> Optional[float]:
    if "USDC" in spot_symbol or "USD" not in spot_symbol or len(spot_symbol) < 6:
        return 0.50
    try:
        js, status = await BINANCE_MGR._fetch_binance(
            session,
            "GET",
            SPOT_24HR,
            params={"symbol": spot_symbol},
            base_url_type="spot",
        )
    except Exception as e:
        log.warning(
            "Spot API fetch failed for %s (treating as neutral): %s", spot_symbol, e
        )
        return 0.50
    if not js:
        return 0.50
    try:
        taker_buy_volume = safe_float(js.get("takerBuyBaseAssetVolume"))
        total_volume = safe_float(js.get("volume"))
        if total_volume > 0:
            return max(0.0, min(1.0, taker_buy_volume / total_volume))
        return 0.50
    except Exception as e:
        log.error("Error calculating Spot Taker Ratio for %s: %s", spot_symbol, e)
        return 0.50


async def get_avg_funding_raw(
    session: aiohttp.ClientSession, fut_symbol: str, limit: int = 21
) -> Optional[float]:
    js, _ = await BINANCE_MGR._fetch_binance(
        session,
        "GET",
        FUT_FUNDING,
        params={"symbol": fut_symbol, "limit": limit},
        base_url_type="futures",
    )
    if not js or not isinstance(js, list) or not js:
        return None
    vals = [safe_float(x.get("fundingRate")) for x in js if "fundingRate" in x]
    if not vals:
        return None
    return sum(vals) / len(vals)


async def get_glsr_class(session: aiohttp.ClientSession, fut_symbol: str) -> str:
    params = {"symbol": fut_symbol, "period": "5m", "limit": 50}
    js, _ = await BINANCE_MGR._fetch_binance(
        session, "GET", FUT_GLSR, params=params, base_url_type="data"
    )
    if not js or not isinstance(js, list) or not js:
        return "glsr_weak"
    ratio = safe_float(js[-1].get("longShortRatio"), 0.5)
    return "glsr_ok" if not (0.48 <= ratio <= 0.52) else "glsr_weak"


async def get_oi_spike_percent_binance_core(
    session: aiohttp.ClientSession, fut_symbol: str
) -> Tuple[float, str]:
    params = {"symbol": fut_symbol, "period": "5m", "limit": 24}
    js, _ = await BINANCE_MGR._fetch_binance(
        session, "GET", FUT_OI_HIST, params=params, base_url_type="data"
    )
    if not js or not isinstance(js, list) or len(js) < 24:
        return 0.0, "BINANCE_FAIL"
    try:
        baseline_data = [safe_float(d.get("sumOpenInterestValue")) for d in js[0:20]]
        recent_data = [safe_float(d.get("sumOpenInterestValue")) for d in js[20:24]]
        if not baseline_data or not recent_data:
            return 0.0, "BINANCE_FAIL"
        baseline_avg = sum(baseline_data) / len(baseline_data)
        recent_avg = sum(recent_data) / len(recent_data)
        if baseline_avg == 0:
            return 0.0, "BINANCE_FAIL"
        spike_pct = ((recent_avg / baseline_avg) - 1.0) * 100.0
        return max(0.0, spike_pct), "BINANCE"
    except Exception as e:
        log.warning("%s (Binance): Error calculating OI spike: %s", fut_symbol, e)
        return 0.0, "BINANCE_FAIL"


async def get_oi_spike_percent_multi(
    session: aiohttp.ClientSession, fut_symbol: str
) -> Tuple[float, str]:
    return await get_oi_spike_percent_binance_core(session, fut_symbol)


async def get_kline_data_binance(
    session: aiohttp.ClientSession, fut_symbol: str, interval: str, limit: int
) -> List[Any]:
    params = {"symbol": fut_symbol, "interval": interval, "limit": limit}
    js, _ = await BINANCE_MGR._fetch_binance(
        session, "GET", FUT_KLINES, params=params, base_url_type="futures"
    )
    if not js or not isinstance(js, list) or len(js) < limit:
        log.warning(
            "Failed to fetch enough K-Line data for %s: Got %d, Need %d",
            fut_symbol,
            len(js),
            limit,
        )
        return []
    return js


def calculate_atr(klines: List[Any], period: int = 14) -> float:
    if not klines or len(klines) < period + 1:
        return 0.0
    klines.sort(key=lambda x: int(x[0]))
    true_ranges = []
    for i in range(1, len(klines)):
        tr = max(
            safe_float(klines[i][2]) - safe_float(klines[i][3]),
            abs(safe_float(klines[i][2]) - safe_float(klines[i - 1][4])),
            abs(safe_float(klines[i][3]) - safe_float(klines[i - 1][4])),
        )
        true_ranges.append(tr)
    if len(true_ranges) < period:
        return 0.0
    initial_atr = sum(true_ranges[0:period]) / period
    atr_values = [initial_atr]
    for i in range(period, len(true_ranges)):
        next_atr = ((atr_values[-1] * (period - 1)) + true_ranges[i]) / period
        atr_values.append(next_atr)
    return atr_values[-1]


def calculate_adaptive_exits(entry_price: float, atr_value: float) -> Dict[str, float]:
    if atr_value <= 0 or entry_price <= 0:
        return {
            "SL": entry_price * 0.95,
            "TP1": entry_price * 1.05,
            "TP2": entry_price * 1.08,
            "TP3": entry_price * 1.10,
        }
    SL_MULTIPLIER = 2.5
    TP1_MULTIPLIER = 1.5
    TP2_MULTIPLIER = 3.0
    TP3_MULTIPLIER = 5.0
    return {
        "SL": entry_price - (SL_MULTIPLIER * atr_value),
        "TP1": entry_price + (TP1_MULTIPLIER * atr_value),
        "TP2": entry_price + (TP2_MULTIPLIER * atr_value),
        "TP3": entry_price + (TP3_MULTIPLIER * atr_value),
    }


async def fetch_advanced_price_magnets(
    session: aiohttp.ClientSession, fut_symbol: str, kline_task: asyncio.Task
) -> Dict[str, Optional[float]]:
    try:
        klines = await kline_task
        current_price = safe_float(klines[-1][4]) if klines and len(klines) > 0 else 1.0
    except Exception as e:
        log.error("kline_task failed, cannot calculate magnets: %s", e)
        current_price = 1.0

    # --- 1. Liquidation Magnet (from local DB) ---
    liquidation_magnet = None
    if current_price > 0:
        try:
            with sqlite3.connect(
                f"file:{LIQUIDATIONS_DB_FILE}?mode=ro", uri=True, timeout=5
            ) as conn:
                time_24h_ago = (
                    datetime.now(timezone.utc) - timedelta(hours=24)
                ).isoformat()
                sql = "SELECT price, notional FROM liquidations WHERE symbol = ? AND ts_utc > ?"
                symbol_liquidations = (
                    conn.cursor().execute(sql, (fut_symbol, time_24h_ago)).fetchall()
                )

                if symbol_liquidations:
                    bin_size = max(0.01, current_price * 0.005)
                    bins = defaultdict(float)
                    for price, notional in symbol_liquidations:
                        bin_price = round(price / bin_size) * bin_size
                        bins[bin_price] += notional
                    if bins:
                        liquidation_magnet = max(bins, key=bins.get)
        except sqlite3.OperationalError:
            log.warning(
                f"'{LIQUIDATIONS_DB_FILE}' not found. Run 'liquidation_tracker.py'."
            )
        except Exception as e:
            log.error(f"Error reading liquidation database: {e}")

    # --- 2. Max Pain (from CoinGlass API) ---
    max_pain_strike = None
    base_asset = fut_symbol.replace("USDT", "").replace("USDC", "")
    if base_asset in ("BTC", "ETH"):
        try:
            url = f"{COINGLASS_V3_BASE_URL}/option/max-pain"
            js, status = await fetch_json(
                session, "GET", url, params={"symbol": base_asset, "exName": "Deribit"}
            )
            if status == 200 and js and js.get("code") == "0":
                max_pain_strike = safe_float(js["data"].get("maxPainPrice"), None)
        except Exception as e:
            log.error(f"Failed to fetch Max Pain data for {base_asset}: {e}")

    return {
        "liquidation_magnet": liquidation_magnet,
        "max_pain_strike": max_pain_strike,
    }


# ----------------- Core Screener Logic (Combined) -----------------


async def get_real_cvd_from_memory(fut_symbol: str) -> Optional[float]:
    """
    Reads the pre-calculated 1-Hour CVD value from the global in-memory state.
    This is a SYNCHRONOUS function.
    """
    global cvd_state_manager, cvd_lock

    if cvd_lock.locked():
        log.warning(f"CVD data for {fut_symbol} is locked. Skipping for this cycle.")
        return None

    try:
        if fut_symbol in cvd_state_manager:
            return cvd_state_manager[fut_symbol]["cvd"]
        else:
            return None  # No data collected yet
    except Exception as e:
        log.error(f"Failed to read CVD from memory for {fut_symbol}: {e}")
        return None


async def evaluate_m1_score(
    session: aiohttp.ClientSession, fut_symbol: str, spot_symbol: str
) -> Dict[str, Any]:

    kline_task = asyncio.create_task(
        get_kline_data_binance(session, fut_symbol, "1h", 15)
    )
    magnet_task = asyncio.create_task(
        fetch_advanced_price_magnets(session, fut_symbol, kline_task)
    )

    # --- v9.0 TUNE: Get CVD from memory
    real_cvd = await get_real_cvd_from_memory(fut_symbol)

    results = await asyncio.gather(
        # --- v9.0 TUNE: Call new function ---
        asyncio.create_task(get_price_change_and_volume(session, fut_symbol)),
        asyncio.create_task(get_avg_funding_raw(session, fut_symbol)),
        asyncio.create_task(get_glsr_class(session, fut_symbol)),
        asyncio.create_task(get_oi_spike_percent_multi(session, fut_symbol)),
        kline_task,
        magnet_task,
        asyncio.create_task(get_spot_taker_ratio(session, spot_symbol)),
    )

    # --- v9.0 TUNE: Unpack new results ---
    (
        (price_change, volume_24h),
        avg_funding,
        glsr_class,
        (oi_spike, oi_source),
        klines,
        magnets,
        spot_taker_ratio,
    ) = results

    current_price = safe_float(klines[-1][4]) if klines and len(klines) > 0 else 1.0
    atr_value = calculate_atr(klines, period=14)
    adaptive_exits = calculate_adaptive_exits(current_price, atr_value)
    risk_pct = (
        max(0.0, ((current_price - adaptive_exits["SL"]) / current_price) * 100.0)
        if current_price > 0
        else 0.0
    )

    score = 0
    if price_change is not None and atr_value > 0 and current_price > 0:
        price_to_atr_ratio = abs(price_change) * current_price / (100.0 * atr_value)
        if price_to_atr_ratio >= 1.0:
            score += 1
        if price_to_atr_ratio >= 2.0:
            score += 1
        if price_to_atr_ratio >= 3.0:
            score += 1
        if price_to_atr_ratio >= 4.0:
            score += 1

    if oi_spike > 10.0:
        score += 3
    if spot_taker_ratio is not None and spot_taker_ratio > 0.55:
        score += 2

    # --- v9.0 TUNE: New Dynamic & Relative CVD Scoring ---
    if real_cvd is not None and volume_24h is not None and volume_24h > 0:
        avg_volume_1h = volume_24h / 24

        # Only check for positive CVD
        if real_cvd > 0 and avg_volume_1h > 0:
            cvd_to_vol_ratio = real_cvd / avg_volume_1h

            # Dynamic Tiered Scoring
            if cvd_to_vol_ratio > 0.1:  # 1H CVD is >10% of avg 1H vol
                score += 1
            if cvd_to_vol_ratio > 0.2:  # 1H CVD is >20% of avg 1H vol
                score += 1
            if cvd_to_vol_ratio > 0.3:  # 1H CVD is >30% of avg 1H vol (Very bullish)
                score += 1
    # --- END NEW LOGIC ---

    if avg_funding is not None and avg_funding < 0.00005:
        score += 1
    if glsr_class == "glsr_ok":
        score += 1

    return {
        "symbol": fut_symbol,
        "score": score,
        "price_change": price_change,
        "oi_spike": oi_spike,
        "oi_source": oi_source,
        "current_price": current_price,
        "ATR": atr_value,
        "SL": adaptive_exits["SL"],
        "TP1": adaptive_exits["TP1"],
        "TP2": adaptive_exits["TP2"],
        "TP3": adaptive_exits["TP3"],
        "risk_pct": risk_pct,
        "liquidation_magnet": magnets.get("liquidation_magnet"),
        "max_pain_strike": magnets.get("max_pain_strike"),
        "spot_taker_ratio": spot_taker_ratio,
        "real_cvd": real_cvd,
        "status": "SCANNED",
    }


async def run_screener_scan(session: aiohttp.ClientSession) -> List[Dict[str, Any]]:
    """The main screener logic, refactored into a function."""
    log.info("Screener: Starting scan...")
    all_scores: List[Dict[str, Any]] = []

    log.info("Screener Step 1: Loading exchange information...")
    fut_map, spot_map, _, _ = await load_all_exchange_maps(session)
    scan_symbols = sorted(fut_map.keys())
    log.info(
        f"Screener Step 2: Starting scan on {len(scan_symbols)} Binance futures symbols..."
    )

    CHUNK_SIZE = 100
    for i in range(0, len(scan_symbols), CHUNK_SIZE):
        chunk = scan_symbols[i : i + CHUNK_SIZE]
        log.info(
            f"Screener: Processing chunk {i//CHUNK_SIZE + 1} of {len(scan_symbols)//CHUNK_SIZE + 1}..."
        )
        chunk_tasks = []
        for fut_symbol in chunk:
            # Handle non-standard symbols that might be in the list
            if not fut_symbol or fut_map.get(fut_symbol) is None:
                continue
            if fut_map[fut_symbol].get("status") != "TRADING":
                continue
            spot_symbol = fut_symbol
            task = evaluate_m1_score(session, fut_symbol, spot_symbol)
            chunk_tasks.append(task)

        if chunk_tasks:
            results = await asyncio.gather(*chunk_tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, Exception):
                    log.error("Screener Task failed with error: %s", res)
                else:
                    all_scores.append(res)
            await asyncio.sleep(1.0)  # Brief pause between chunks

    return all_scores


def print_results_table(all_scores: List[Dict[str, Any]]):
    """Formats and prints the final results in a colorized table."""
    high_score_symbols = [s for s in all_scores if s["score"] > 0]
    log.info("-" * 40)
    log.info(f"✅ Scan Complete. Total Symbols Scanned: {len(all_scores)}")

    if high_score_symbols:
        sorted_symbols = sorted(
            high_score_symbols,
            key=lambda s: (
                s["score"],
                s["price_change"] if s["price_change"] is not None else -1000.0,
            ),
            reverse=True,
        )
        top_10_symbols = sorted_symbols[:10]
        colorama.init(autoreset=True)
        log.info(
            f"🏆 Found {len(high_score_symbols)} potential movers. Displaying Top {len(top_10_symbols)}:"
        )

        table_data = []
        for i, s in enumerate(top_10_symbols):
            price_change = s["price_change"]
            if price_change is None:
                price_display = "N/A"
            elif price_change > 0:
                price_display = f"⬆️ {colorama.Fore.GREEN}{price_change:.2f}%{colorama.Style.RESET_ALL}"
            else:
                price_display = f"⬇️ {colorama.Fore.RED}{price_change:.2f}%{colorama.Style.RESET_ALL}"

            oi_spike = s["oi_spike"]
            if oi_spike > 5.0:
                oi_display = f"{colorama.Fore.YELLOW}{oi_spike:.2f}% ({s['oi_source']}){colorama.Style.RESET_ALL}"
            else:
                oi_display = f"{colorama.Style.DIM}{oi_spike:.2f}% ({s['oi_source']}){colorama.Style.RESET_ALL}"

            spot_taker = s["spot_taker_ratio"]
            if spot_taker > 0.55:
                spot_taker_display = f"🔥 {colorama.Fore.GREEN}{spot_taker:.2f}{colorama.Style.RESET_ALL}"
            elif spot_taker < 0.45:
                spot_taker_display = (
                    f"🩸 {colorama.Fore.RED}{spot_taker:.2f}{colorama.Style.RESET_ALL}"
                )
            else:
                spot_taker_display = (
                    f"⚪ {colorama.Style.DIM}{spot_taker:.2f}{colorama.Style.RESET_ALL}"
                )

            real_cvd = s["real_cvd"]
            if real_cvd is None:
                cvd_display = f"{colorama.Style.DIM}N/A{colorama.Style.RESET_ALL}"
            elif real_cvd > 0:
                cvd_display = f"📈 {colorama.Fore.GREEN}${real_cvd:,.0f}{colorama.Style.RESET_ALL}"
            else:
                cvd_display = (
                    f"📉 {colorama.Fore.RED}${real_cvd:,.0f}{colorama.Style.RESET_ALL}"
                )

            liq_magnet = s["liquidation_magnet"]
            liq_magnet_display = (
                f"🧲 {colorama.Fore.CYAN}{liq_magnet:.4f}{colorama.Style.RESET_ALL}"
                if liq_magnet
                else f"{colorama.Style.DIM}N/A{colorama.Style.RESET_ALL}"
            )

            max_pain = s["max_pain_strike"]
            max_pain_display = (
                f"📍 {colorama.Fore.YELLOW}{max_pain:.2f}{colorama.Style.RESET_ALL}"
                if max_pain
                else f"{colorama.Style.DIM}N/A{colorama.Style.RESET_ALL}"
            )

            row = [
                f"#{i+1}",
                f"{colorama.Fore.WHITE}{s['symbol']}{colorama.Style.RESET_ALL}",
                f"{s['score']}",
                price_display,
                oi_display,
                spot_taker_display,
                cvd_display,
                f"{s['current_price']:.4f}",
                f"{colorama.Fore.CYAN}{s['risk_pct']:.2f}%{colorama.Style.RESET_ALL}",
                liq_magnet_display,
                max_pain_display,
                f"{colorama.Fore.RED}{s['SL']:.4f}{colorama.Style.RESET_ALL}",
                f"{colorama.Fore.GREEN}{s['TP1']:.4f}{colorama.Style.RESET_ALL}",
                f"{colorama.Fore.CYAN}{s['TP2']:.4f}{colorama.Style.RESET_ALL}",
                f"{colorama.Fore.CYAN}{s['TP3']:.4f}{colorama.Style.RESET_ALL}",
            ]
            table_data.append(row)

        headers = [
            "Rank",
            "Symbol",
            "Score",
            "24H Price",
            "OI Spike",
            "Spot Taker",
            "1H CVD ($)",
            "Entry Price",
            "Max Risk %",
            "Liq. Magnet",
            "Max Pain",
            "SL",
            "TP1",
            "TP2",
            "TP3",
        ]
        print(
            "\n"
            + tabulate(
                table_data, headers=headers, tablefmt="fancy_grid", numalign="right"
            )
        )
        colorama.deinit()

    else:
        log.info("No symbols found with a score greater than 0.")

    log.info("-" * 40)


# ----------------------------- Main Execution Block -----------------------------


async def main():
    log.info("Starting Vertical Explosion Catcher v9.0 (Tuned Engine)...")

    # 1. Start the background CVD engine task
    cvd_engine_task = asyncio.create_task(background_cvd_engine())

    # 2. Give the engine a moment to connect and start fetching data
    log.info("Waiting 10 seconds for CVD Engine to initialize...")
    await asyncio.sleep(10)

    # 3. Run the main screener logic
    all_scores = []
    try:
        async with aiohttp.ClientSession(
            headers=HEADERS, timeout=HTTP_TIMEOUT
        ) as session:
            all_scores = await run_screener_scan(session)
    except Exception as e:
        log.error(f"Main screener scan failed: {e}")
    finally:
        # 4. Stop the background task and exit
        log.info("Screener scan finished. Shutting down CVD engine...")
        cvd_engine_task.cancel()
        try:
            await cvd_engine_task
        except asyncio.CancelledError:
            log.info("CVD Engine task successfully cancelled.")

    # 5. Print the final results
    print_results_table(all_scores)

    log.info("Vertical Explosion Catcher v9.0 finished successfully.")


if __name__ == "__main__":
    try:
        log.info("Starting asyncio event loop...")
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Script manually stopped by user (KeyboardInterrupt).")
    except Exception as e:
        log.error(f"An unexpected error occurred during execution: %s", e)
        # Explicitly print traceback for debugging
        import traceback

        traceback.print_exc()
