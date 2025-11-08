# ##############################################################################
# ## Robust 4-Hour Pump Bot (v6.3.4 - Syntax Fix)                         ##
# ## Features: L1 (Historical Scan), L2 (LIVE Confidence Score), Vetoes,  ##
# ##           Gap-Safe WS, Probabilistic Sizing, Sim Mode.               ##
# ##           UPGRADES:                                                  ##
# ##           1. Gap-detection on WS streams (cite: Streams.md)          ##
# ##           2. L2 Score now uses "Real" 5m CVD (cite: Streams.md)      ##
# ##           3. BUG FIX: Corrected depth stream key from 'lastUpdateId' to 'u'
# ##           4. VETO: Added OI Veto to L2 Score (RVN Trap Fix)          ##
# ##           5. FIX: Corrected SyntaxError on line 1034                 ##
# ##           6. FIX: Removed stray '}' at end of file                   ##
# ##############################################################################

import asyncio
import argparse
import sys
import time
import logging
import hashlib
import aiosqlite
import hmac
import configparser
import json
import websockets
from collections import deque, defaultdict
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional, Any, Tuple

import aiohttp
import numpy as np
import pandas as pd
import pandas_ta as ta
import rfc8785
from tabulate import tabulate
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
    retry_if_exception_type,
)


# --- ANSI Color Codes for terminal output ---
class Colors:
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    RED = "\033[91m"
    CYAN = "\033[96m"
    MAGENTA = "\033[95m"
    BLUE = "\033[94m"
    BOLD = "\033[1m"
    END = "\033[0m"


# --- CONFIGURATION (Defaults) ---
DB_FILE = "data/4h_pump_ledger.db"
LOG_FILE = "logs/bot.log"
RECENCY_HALF_LIFE_MIN = 45
SCAN_INTERVAL_SECONDS = 300  # 5 minutes

# --- L1 Filter Config (Curation) ---
L1_LOOKBACK_DAYS = 5
L1_MIN_SPOT_ACCUMULATION_PCT = 15.0
L1_MAX_FUNDING_RATE = 0.001  # (0.1%)
L1_HTF_TREND_PERIOD = 50  # (50-EMA on 4h chart)

# --- L2 Filter Config (Confidence Scoring) ---
L2_MOMENTUM_LOOKBACK = 30  # 5m bins (Now from LIVE data)
L2_MIN_CONFIDENCE_SCORE = 50.0  # (Min score to be considered a signal)
L2_BB_EXPANSION_THRESHOLD = 0.02  # (2% BB expansion)
L2_OI_TREND_PERIOD = 5  # (5-bar SMA of OI delta)

# --- Base Universe Config ---
L3_MAX_FUT_SPOT_VOL_RATIO = 4.0
MIN_24H_NOTIONAL_VOLUME = 10_000_000
MAX_BID_ASK_SPREAD_PCT = 0.1

# --- Microstructure Trigger Config ---
MICROSTRUCTURE_WINDOW = 100
CVD_ZSCORE_WINDOW = 200
CVD_ZSCORE_THRESHOLD = 2.5
CVD_ZSCORE_MIN_SAMPLES = 50
TAKER_RATIO_THRESHOLD = 0.60
MAX_STREAMS_PER_WS_CONNECTION = 100
LIQUIDITY_STREAM_TYPE = "depth5@100ms"
LIQUIDITY_CLUSTER_MIN_SIZE_MULTIPLIER = 10

# --- Trade Execution & Veto Config ---
VETO_MIN_TP1_RR = 2.5
VETO_LIQUIDITY_CAP_PCT = 0.01
VETO_LIQUIDITY_CAP_BARS = 20
PROB_CONFIDENCE_THRESHOLD = 75.0
PROB_UPSCALE_MULTIPLIER = 1.4

API_CONFIG = {
    "binance": {
        "fapi": "https://fapi.binance.com",
        "api": "https://api.binance.com",
        "rate_limit": (2000, 60),  # weight/min
        "tf_map": {"1m": "1m", "5m": "5m", "15m": "15m", "1d": "1d", "4h": "4h"},
    }
}

# --- LOGGING & CONSOLE SETUP ---
# --- FIX: Create log/data directories *before* initializing logging ---
import os
os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)
# --- End Fix ---

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(sys.stdout)
    ],
)


# --- UTILITY CLASSES ---
class AsyncTokenBucket:
    def __init__(self, rate: float, capacity: int):
        self.rate, self.capacity, self._tokens, self.last_refill = (
            rate,
            capacity,
            float(capacity),
            time.monotonic(),
        )
        self.lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1):
        async with self.lock:
            now = time.monotonic()
            elapsed = now - self.last_refill
            if elapsed > 0:
                self._tokens = min(self.capacity, self._tokens + (elapsed * self.rate))
                self.last_refill = now
            while self._tokens < tokens:
                await asyncio.sleep(0.1)
                await self.acquire(tokens=0)
            self._tokens -= tokens


class DBManager:
    @staticmethod
    async def initialize():
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute("PRAGMA journal_mode=WAL;")
            # --- MODIFIED: L1/L2 scores are now separate ---
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS signal_ledger (
                    event_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, exchange TEXT NOT NULL,
                    event_time INTEGER NOT NULL, l1_score REAL, l2_score REAL, status TEXT,
                    metrics TEXT
                )
            """
            )
            # --- NEW: Table for L1 (historical) results ---
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS l1_pass_symbols (
                    symbol TEXT PRIMARY KEY,
                    l1_score REAL NOT NULL,
                    metrics TEXT NOT NULL,
                    last_passed_time INTEGER NOT NULL
                )
                """
            )
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS active_trades (
                    trade_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    status TEXT NOT NULL,
                    entry_price REAL,
                    stop_loss_price REAL,
                    take_profit_price REAL,
                    position_size REAL,
                    entry_time INTEGER,
                    exit_time INTEGER,
                    exit_reason TEXT,
                    tp_order_id TEXT,
                    sl_order_id TEXT,
                    FOREIGN KEY (event_id) REFERENCES signal_ledger (event_id)
                )
            """
            )
            await db.commit()
    
    # --- NEW: Write L1 historical scan results ---
    @staticmethod
    async def write_l1_pass(symbol: str, l1_score: float, metrics: str):
        now = int(datetime.now(timezone.utc).timestamp())
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                """
                INSERT INTO l1_pass_symbols (symbol, l1_score, metrics, last_passed_time)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(symbol) DO UPDATE SET
                    l1_score = excluded.l1_score,
                    metrics = excluded.metrics,
                    last_passed_time = excluded.last_passed_time
                """,
                (symbol, l1_score, metrics, now),
            )
            await db.commit()

    # --- NEW: Get all symbols that passed L1 ---
    @staticmethod
    async def get_l1_pass_symbols() -> List[aiosqlite.Row]:
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM l1_pass_symbols")
            return await cursor.fetchall()

    # --- NEW: Get a single L1 symbol ---
    @staticmethod
    async def get_l1_pass_symbol(symbol: str) -> Optional[aiosqlite.Row]:
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM l1_pass_symbols WHERE symbol = ?", (symbol,))
            return await cursor.fetchone()

    @staticmethod
    async def write_event(
        event_id, symbol, exchange, event_time, l1_score, l2_score, metrics
    ):
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "INSERT OR IGNORE INTO signal_ledger VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    event_id,
                    symbol,
                    exchange,
                    int(event_time.timestamp()),
                    l1_score,
                    l2_score,
                    "DETECTED",
                    str(metrics),
                ),
            )
            await db.commit()

    @staticmethod
    async def update_status(event_id, status):
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "UPDATE signal_ledger SET status = ? WHERE event_id = ?",
                (status, event_id),
            )
            await db.commit()

    @staticmethod
    async def query_recent_events():
        # This function is now less critical as events are live
        # We'll query events from the last 24h just for the display table
        cutoff = int(
            (
                datetime.now(timezone.utc) - timedelta(hours=24)
            ).timestamp()
        )
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM signal_ledger WHERE event_time >= ? ORDER BY event_time DESC",
                (cutoff,),
            )
            return await cursor.fetchall()

    @staticmethod
    async def get_event_by_id(event_id: str):
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM signal_ledger WHERE event_id = ?", (event_id,)
            )
            return await cursor.fetchone()

    @staticmethod
    async def open_new_trade(
        event_id,
        symbol,
        entry_price,
        sl_price,
        tp_price,
        position_size,
        tp_order_id,
        sl_order_id,
    ):
        entry_time = int(datetime.now(timezone.utc).timestamp())
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                """INSERT INTO active_trades
                   (event_id, symbol, status, entry_price, stop_loss_price, take_profit_price, position_size, entry_time, tp_order_id, sl_order_id)
                   VALUES (?, ?, 'LIVE', ?, ?, ?, ?, ?, ?, ?)""",
                (
                    event_id,
                    symbol,
                    entry_price,
                    sl_price,
                    tp_price,
                    position_size,
                    entry_time,
                    tp_order_id,
                    sl_order_id,
                ),
            )
            await db.commit()

    @staticmethod
    async def close_trade(trade_id, exit_reason):
        exit_time = int(datetime.now(timezone.utc).timestamp())
        async with aiosqlite.connect(DB_FILE) as db:
            await db.execute(
                "UPDATE active_trades SET status = 'CLOSED', exit_time = ?, exit_reason = ? WHERE trade_id = ?",
                (exit_time, exit_reason, trade_id),
            )
            await db.commit()

    @staticmethod
    async def get_live_trades():
        async with aiosqlite.connect(DB_FILE) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT * FROM active_trades")
            return await cursor.fetchall()


# --- WebSocketDataManager CLASS (v6.3.1 - Gap Detection & 5m Bins) ---
class WebSocketDataManager:
    def __init__(self):
        self.symbols = []
        self.base_url = "wss://fstream.binance.com/stream?streams="
        self.cvd_deltas = defaultdict(lambda: deque(maxlen=MICROSTRUCTURE_WINDOW))
        self.historical_kicks = defaultdict(lambda: deque(maxlen=CVD_ZSCORE_WINDOW))
        self.taker_buy_vol = defaultdict(lambda: deque(maxlen=MICROSTRUCTURE_WINDOW))
        self.taker_sell_vol = defaultdict(lambda: deque(maxlen=MICROSTRUCTURE_WINDOW))
        self._trigger_status = defaultdict(bool)
        self.order_book_data = defaultdict(dict)
        self._lock = asyncio.Lock()
        self._connection_tasks = []
        self.logger = logging.getLogger("ws_triggers")

        # --- UPGRADE 1: Gap Detection & Stream Health ---
        #]
        self.last_aggtrade_id = defaultdict(int)
        self.last_depth_update_id = defaultdict(int)
        self.stream_health_status = defaultdict(bool)
        
        # --- UPGRADE 2: Real CVD 5m Binning ---
        self.cvd_5m_bin_size = L2_MOMENTUM_LOOKBACK
        self.cvd_5m_bins = defaultdict(lambda: deque(maxlen=self.cvd_5m_bin_size))
        self.current_5m_cvd = defaultdict(float)
        self.last_5m_bin_time = 0
        self.cvd_snapshot = defaultdict(lambda: {"cvd_5m_trend": 0.0})
        # --- End Upgrades ---

    async def update_symbols(self, new_symbols: List[str]):
        self.symbols = [s.lower() for s in new_symbols]
        await self._reinitialize_data()

    async def _reinitialize_data(self):
        async with self._lock:
            current_symbols = set(self.symbols)
            
            for symbol in list(self.cvd_deltas.keys()):
                if symbol not in current_symbols:
                    del self.cvd_deltas[symbol]
                    del self._trigger_status[symbol]
                    del self.historical_kicks[symbol]
                    del self.taker_buy_vol[symbol]
                    del self.taker_sell_vol[symbol]
                    if symbol in self.order_book_data:
                        del self.order_book_data[symbol]
                    # Clean up gap detection dicts
                    if symbol in self.last_aggtrade_id:
                        del self.last_aggtrade_id[symbol]
                    if symbol in self.last_depth_update_id:
                        del self.last_depth_update_id[symbol]
                    if symbol in self.stream_health_status:
                        del self.stream_health_status[symbol]
                    # Clean up 5m bin dicts
                    if symbol in self.cvd_5m_bins:
                        del self.cvd_5m_bins[symbol]
                    if symbol in self.current_5m_cvd:
                        del self.current_5m_cvd[symbol]
                    if symbol in self.cvd_snapshot:
                        del self.cvd_snapshot[symbol]
            
            for symbol in current_symbols:
                if symbol not in self.cvd_deltas:
                    self.cvd_deltas[symbol] = deque(maxlen=MICROSTRUCTURE_WINDOW)
                    self._trigger_status[symbol] = False
                    self.historical_kicks[symbol] = deque(maxlen=CVD_ZSCORE_WINDOW)
                    self.taker_buy_vol[symbol] = deque(maxlen=MICROSTRUCTURE_WINDOW)
                    self.taker_sell_vol[symbol] = deque(maxlen=MICROSTRUCTURE_WINDOW)
                    self.order_book_data[symbol] = {}
                    # Initialize gap detection
                    self.last_aggtrade_id[symbol] = 0
                    self.last_depth_update_id[symbol] = 0
                    self.stream_health_status[symbol] = True
                    # Initialize 5m bin
                    self.cvd_5m_bins[symbol] = deque(maxlen=self.cvd_5m_bin_size)
                    self.current_5m_cvd[symbol] = 0.0
                    self.cvd_snapshot[symbol] = {"cvd_5m_trend": 0.0}

    async def start_connections(self):
        if not self.symbols:
            logging.warning("WebSocketDataManager: No symbols to monitor.")
            return

        await self.stop_connections()

        self.symbol_batches = [
            self.symbols[i : i + MAX_STREAMS_PER_WS_CONNECTION]
            for i in range(0, len(self.symbols), MAX_STREAMS_PER_WS_CONNECTION)
        ]

        logging.info(
            f"Starting {len(self.symbol_batches)} WebSocket connection(s) for {len(self.symbols)} symbols..."
        )

        for i, batch in enumerate(self.symbol_batches):
            agg_trade_task = asyncio.create_task(
                self.connect_and_listen(batch, i, "aggTrade")
            )
            depth_task = asyncio.create_task(
                self.connect_and_listen(batch, i, LIQUIDITY_STREAM_TYPE)
            )
            self._connection_tasks.append(agg_trade_task)
            self._connection_tasks.append(depth_task)
        
        # --- NEW: Start 5m binning task ---
        self._connection_tasks.append(
            asyncio.create_task(self._cvd_binner_task())
        )
        # --- End New ---

        await asyncio.sleep(5)

    async def stop_connections(self):
        if not self._connection_tasks:
            return
        logging.info(
            f"Stopping {len(self._connection_tasks)} WebSocket connection tasks..."
        )
        for task in self._connection_tasks:
            task.cancel()
        try:
            await asyncio.gather(*self._connection_tasks, return_exceptions=True)
        except asyncio.CancelledError:
            pass
        self._connection_tasks = []
        logging.info("WebSocket connections stopped.")

    # --- NEW: Task to bin CVD every 5 minutes ---
    async def _cvd_binner_task(self):
        """
        This background task runs every 5 minutes to bin the accumulated
        CVD and update the trend snapshot for the L2 score.
        """
        while True:
            # Wait for the next 5-minute mark
            now = datetime.now(timezone.utc)
            next_run = (now + timedelta(minutes=5)).replace(minute=(now.minute // 5) * 5, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(minutes=5)
            
            wait_seconds = (next_run - now).total_seconds()
            await asyncio.sleep(wait_seconds)
            
            self.last_5m_bin_time = next_run.timestamp()
            logging.debug(f"Binning 5m CVD for {len(self.symbols)} symbols...")
            
            async with self._lock:
                for symbol in self.symbols:
                    if not self.stream_health_status.get(symbol, True):
                        continue # Don't bin corrupt data
                    
                    # Get the CVD for the last 5m and reset the counter
                    binned_cvd = self.current_5m_cvd[symbol]
                    self.current_5m_cvd[symbol] = 0.0
                    
                    # Add to the deque
                    self.cvd_5m_bins[symbol].append(binned_cvd)
                    
                    # Update the snapshot for L2 scoring
                    if len(self.cvd_5m_bins[symbol]) > 0:
                        self.cvd_snapshot[symbol] = {
                            "cvd_5m_trend": sum(self.cvd_5m_bins[symbol])
                        }

    async def connect_and_listen(
        self, symbol_batch: List[str], batch_id: int, stream_type: str
    ):
        streams = [f"{s}@{stream_type}" for s in symbol_batch]
        stream_path = "/".join(streams)
        url = self.base_url + stream_path

        try:
            total_batches = len(self.symbol_batches)
            logging.info(
                f"Connecting WS Batch {batch_id+1}/{total_batches} ({stream_type} streams)..."
            )
        except AttributeError:
            logging.info(f"Connecting WS Batch {batch_id+1} ({stream_type} streams)...")

        while True:
            try:
                async with websockets.connect(url) as ws:
                    logging.info(
                        f"WebSocket Batch {batch_id+1} ({stream_type}) Connected."
                    )
                    # --- MODIFIED: Reset health on new connection ---
                    async with self._lock:
                        for symbol_lower in symbol_batch:
                            self.stream_health_status[symbol_lower] = True
                            self.last_aggtrade_id[symbol_lower] = 0
                            self.last_depth_update_id[symbol_lower] = 0
                            # Don't reset 5m bin data, just health
                    logging.info(f"Stream health for Batch {batch_id+1} reset to healthy.")
                    # --- End Modified ---

                    while True:
                        message = await ws.recv()
                        data = json.loads(message)
                        if "stream" in data and "data" in data:
                            await self.process_message(data["stream"], data["data"])

            except asyncio.CancelledError:
                logging.info(f"WS Batch {batch_id+1} ({stream_type}) task cancelled.")
                break
            except websockets.exceptions.ConnectionClosedError as e:
                logging.error(
                    f"WS Batch {batch_id+1} ({stream_type}) connection closed: {e}. Reconnecting in 5s..."
                )
                await asyncio.sleep(5)
            except Exception as e:
                logging.error(
                    f"WS Batch {batch_id+1} ({stream_type}) error: {e}. Reconnecting in 5s..."
                )
                await asyncio.sleep(5)

    async def process_message(self, stream_name: str, data: Dict):
        try:
            parts = stream_name.split("@")
            symbol = parts[0]
            stream_type = parts[1]

            # --- UPGRADE 1: Health Check ---
            if not self.stream_health_status.get(symbol, True):
                return
            # --- End Upgrade 1 ---

            if stream_type == "aggTrade":
                # --- UPGRADE 1: Sequence Gap Detection ---
                #]
                agg_trade_id = data.get('a')
                if agg_trade_id is None:
                    logging.warning(f"No aggregate trade ID 'a' in message for {symbol}. Skipping.")
                    return

                last_trade_id = self.last_aggtrade_id[symbol]

                if last_trade_id != 0 and agg_trade_id != last_trade_id + 1:
                    self.stream_health_status[symbol] = False
                    logging.critical(
                        f"CRITICAL: GAP DETECTED in {symbol.upper()} aggTrade stream. "
                        f"Expected ID {last_trade_id + 1} but got {agg_trade_id}. "
                        f"CVD data for this symbol is now CORRUPT and will be disabled."
                    )
                    return 
                
                self.last_aggtrade_id[symbol] = agg_trade_id
                # --- End Upgrade 1 ---

                qty = float(data.get("q"))
                is_buyer_maker = data.get("m")
                cvd_delta = qty if not is_buyer_maker else -qty
                taker_buy = qty if not is_buyer_maker else 0.0
                taker_sell = qty if is_buyer_maker else 0.0

                async with self._lock:
                    # L3 Trigger (Microstructure)
                    self.cvd_deltas[symbol].append(cvd_delta)
                    self.taker_buy_vol[symbol].append(taker_buy)
                    self.taker_sell_vol[symbol].append(taker_sell)
                    
                    # --- UPGRADE 2: Accumulate for 5m bin ---
                    self.current_5m_cvd[symbol] += cvd_delta
                    # --- End Upgrade 2 ---
                    
                    await self.check_microstructure_triggers(symbol)

            elif stream_type == "depth5":
                # --- BUG FIX: The key is 'u' (final update ID), not 'lastUpdateId' ---
                update_id = data.get('u')
                if update_id is None:
                     # This log will no longer spam, but will appear if the payload is malformed
                     logging.warning(f"No 'u' (final update ID) in depth message for {symbol}. Skipping.")
                     return

                last_update_id = self.last_depth_update_id[symbol]
                
                if update_id <= last_update_id:
                    # Stale or out-of-order message, ignore it
                    return
                
                self.last_depth_update_id[symbol] = update_id
                # --- End Bug Fix ---

                async with self._lock:
                    self.order_book_data[symbol] = {
                        "bids": [[float(p), float(q)] for p, q in data.get("b", [])],
                        "asks": [[float(p), float(q)] for p, q in data.get("a", [])],
                        "ts": time.time(),
                    }

        except Exception as e:
            logging.warning(f"Error processing WS message: {e} | Data: {data}")

    async def check_microstructure_triggers(self, symbol: str):
        if not self.stream_health_status.get(symbol, True):
            return

        current_cvd_kick = sum(self.cvd_deltas[symbol])
        self.historical_kicks[symbol].append(current_cvd_kick)

        cvd_z_triggered = False
        if len(self.historical_kicks[symbol]) >= CVD_ZSCORE_MIN_SAMPLES:
            history = np.array(self.historical_kicks[symbol])
            mean_kick = np.mean(history)
            std_dev_kick = np.std(history)

            if std_dev_kick > 0:
                z_score = (current_cvd_kick - mean_kick) / std_dev_kick
                if z_score > CVD_ZSCORE_THRESHOLD:
                    cvd_z_triggered = True
                    if not self._trigger_status[symbol]:
                        self.logger.info(
                            f"{Colors.MAGENTA}🔥 Z-SCORE CVD KICK for {symbol.upper()}!{Colors.END}"
                            f" (Kick: {current_cvd_kick:.2f}, Z: {z_score:.2f} > {CVD_ZSCORE_THRESHOLD})"
                        )

        taker_ratio_triggered = False
        if len(self.taker_buy_vol[symbol]) == MICROSTRUCTURE_WINDOW:
            total_buy_vol = sum(self.taker_buy_vol[symbol])
            total_sell_vol = sum(self.taker_sell_vol[symbol])
            total_taker_vol = total_buy_vol + total_sell_vol

            if total_taker_vol > 0:
                taker_buy_ratio = total_buy_vol / total_taker_vol
                if taker_buy_ratio > TAKER_RATIO_THRESHOLD:
                    taker_ratio_triggered = True
                    if not self._trigger_status[symbol]:
                        self.logger.info(
                            f"{Colors.BLUE}📈 TAKER RATIO SPIKE for {symbol.upper()}!{Colors.END}"
                            f" (Ratio: {taker_buy_ratio:.1%} > {TAKER_RATIO_THRESHOLD:.1%})"
                        )

        self._trigger_status[symbol] = cvd_z_triggered or taker_ratio_triggered

    async def get_trigger_status(self, symbol: str) -> bool:
        symbol_lower = symbol.lower()
        
        if not self.stream_health_status.get(symbol_lower, True):
            return False

        if symbol_lower not in self._trigger_status:
            return False

        async with self._lock:
            triggered = self._trigger_status[symbol_lower]
            if triggered:
                self._trigger_status[symbol_lower] = False
            return triggered

    async def find_liquidity_clusters(
        self, symbol: str, notional_risk_usd: float
    ) -> Optional[Dict]:
        symbol_lower = symbol.lower()
        async with self._lock:
            book = self.order_book_data.get(symbol_lower)

        if not book:
            return None

        min_cluster_size_usd = notional_risk_usd * LIQUIDITY_CLUSTER_MIN_SIZE_MULTIPLIER

        best_bid_cluster_price = None
        best_ask_cluster_price = None

        for price, quantity in book.get("bids", []):
            order_notional = price * quantity
            if order_notional >= min_cluster_size_usd:
                best_bid_cluster_price = max(best_bid_cluster_price or 0, price)

        for price, quantity in book.get("asks", []):
            order_notional = price * quantity
            if order_notional >= min_cluster_size_usd:
                best_ask_cluster_price = min(
                    best_ask_cluster_price or float("inf"), price
                )

        return {
            "bid_cluster": best_bid_cluster_price,
            "ask_cluster": best_ask_cluster_price,
        }

    # --- NEW: Getter for L2 Score ---
    async def get_l2_snapshot_data(self, symbol: str) -> Optional[Dict]:
        """
        Gets the 'real' L2 data for a symbol.
        Returns None if the stream is corrupt.
        """
        symbol_lower = symbol.lower()
        if not self.stream_health_status.get(symbol_lower, True):
            return None

        async with self._lock:
            # Return a copy to avoid race conditions
            return self.cvd_snapshot.get(symbol_lower).copy()
    # --- End New ---


# --- EXCHANGE MANAGER & DATA FETCHING (Unchanged, includes circuit breaker) ---
class BinanceManager:
    def __init__(self, session: aiohttp.ClientSession, api_key: str, api_secret: str):
        self.name = "binance"
        self.session = session
        self.config = API_CONFIG[self.name]
        self.api_key = api_key
        self.api_secret = api_secret
        rate, period = self.config["rate_limit"]
        self.limiter = AsyncTokenBucket(rate / period, rate)
        
        self.logger = logging.getLogger(f"BinanceManager.{self.name}")
        self.ban_expiry = 0

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=1, max=10),
        retry=retry_if_exception_type(aiohttp.ClientError),
    )
    async def _fetch(
        self, url: str, params: Optional[Dict] = None, weight: int = 1
    ) -> Any:
        
        now = time.monotonic()
        if now < self.ban_expiry:
            wait_time = self.ban_expiry - now
            self.logger.error(
                f"[CIRCUIT BREAKER] Tripped! Halting all new requests for {wait_time:.1f}s."
            )
            await asyncio.sleep(wait_time)
            self.logger.info("[CIRCUIT BREAKER] Reset. Resuming requests.")

        await self.limiter.acquire(weight)
        try:
            async with self.session.get(url, params=params, timeout=20) as response:
                if response.status in [429, 418]:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    
                    ban_duration = retry_after + 5
                    self.ban_expiry = time.monotonic() + ban_duration
                    self.logger.error(
                        f"[CIRCUIT BREAKER] {response.status} Error ({'IP Ban' if response.status == 418 else 'Rate Limit'}). "
                        f"Tripping circuit for {ban_duration}s."
                    )
                    
                    await asyncio.sleep(retry_after + 1)
                    response.raise_for_status()
                
                response.raise_for_status()

                if 'X-MBX-USED-WEIGHT-1M' in response.headers:
                    used_weight = response.headers['X-MBX-USED-WEIGHT-1M']
                    self.logger.debug(f"Request successful. Used Weight (1m): {used_weight}")

                return await response.json()
        except Exception as e:
            self.logger.error(f"Failed to fetch {url}: {e}")
            raise

    async def get_klines(self, symbol, interval, limit, is_f=True):
        base = self.config["fapi"] if is_f else self.config["api"]
        endpoint = "/fapi/v1/klines" if is_f else "/api/v3/klines"
        weight = 2 if limit > 100 else 1
        tf = self.config["tf_map"].get(interval, interval)
        return await self._fetch(
            f"{base}{endpoint}",
            {"symbol": symbol, "interval": tf, "limit": limit},
            weight,
        )

    async def get_oi_history(self, symbol, period, limit):
        return await self._fetch(
            f"{self.config['fapi']}/futures/data/openInterestHist",
            {"symbol": symbol, "period": period, "limit": limit},
            1,
        )

    async def get_all_tickers_24h(self, is_f=True):
        base = self.config["fapi"] if is_f else self.config["api"]
        endpoint = "/fapi/v1/ticker/24hr" if is_f else "/api/v3/ticker/24hr"
        return await self._fetch(f"{base}{endpoint}", weight=40)

    async def get_funding_rates(self):
        return await self._fetch(
            f"{self.config['fapi']}/fapi/v1/premiumIndex", weight=1
        )

    async def get_all_book_tickers(self):
        return await self._fetch(
            f"{self.config['fapi']}/fapi/v1/ticker/bookTicker", weight=2
        )

    async def get_all_perp_info(self):
        return await self._fetch(
            f"{self.config['fapi']}/fapi/v1/exchangeInfo", weight=1
        )

    def _create_signature(self, params: Dict) -> str:
        query_string = "&".join(f"{k}={v}" for k, v in params.items())
        return hmac.new(
            self.api_secret.encode("utf-8"),
            query_string.encode("utf-8"),
            hashlib.sha256,
        ).hexdigest()

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_random_exponential(multiplier=1, max=10),
        retry=retry_if_exception_type(aiohttp.ClientError),
    )
    async def _post_signed_order(
        self, endpoint: str, params: Dict, weight: int = 1
    ) -> Any:
        
        now = time.monotonic()
        if now < self.ban_expiry:
            wait_time = self.ban_expiry - now
            self.logger.error(
                f"[CIRCUIT BREAKER] Tripped! Halting all new requests for {wait_time:.1f}s."
            )
            await asyncio.sleep(wait_time)
            self.logger.info("[CIRCUIT BREAKER] Reset. Resuming requests.")

        await self.limiter.acquire(weight)
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._create_signature(params)
        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"{self.config['fapi']}{endpoint}"
        try:
            async with self.session.post(
                url, headers=headers, data=params, timeout=20
            ) as response:
                if response.status >= 400:
                    self.logger.error(
                        f"Binance order error {response.status}: {await response.text()}"
                    )
                    if response.status in [429, 418]:
                        retry_after = int(response.headers.get("Retry-After", "60"))
                        
                        ban_duration = retry_after + 5
                        self.ban_expiry = time.monotonic() + ban_duration
                        self.logger.error(
                            f"[CIRCUIT BREAKER] {response.status} Error ({'IP Ban' if response.status == 418 else 'Rate Limit'}). "
                            f"Tripping circuit for {ban_duration}s."
                        )

                        await asyncio.sleep(retry_after + 1)
                    response.raise_for_status()
                
                if 'X-MBX-USED-WEIGHT-1M' in response.headers:
                    used_weight = response.headers['X-MBX-USED-WEIGHT-1M']
                    self.logger.debug(f"Order successful. Used Weight (1m): {used_weight}")
                
                return await response.json()
        except Exception as e:
            self.logger.error(f"Failed to post signed order: {e}")
            raise

    async def place_market_order(
        self, symbol: str, side: str, quantity: float
    ) -> Optional[Dict]:
        params = {
            "symbol": symbol,
            "side": side,
            "type": "MARKET",
            "quantity": f"{quantity:.3f}",
        }
        try:
            return await self._post_signed_order("/fapi/v1/order", params, weight=1)
        except Exception as e:
            self.logger.error(f"Failed to place market order for {symbol}: {e}")
            return None

    async def place_oco_order(
        self,
        symbol: str,
        quantity: float,
        take_profit_price: float,
        stop_loss_price: float,
    ) -> Optional[Dict]:
        self.logger.info(
            f"Placing OCO (2 orders) for {symbol}: TP={take_profit_price}, SL={stop_loss_price}"
        )
        try:
            tp_params = {
                "symbol": symbol,
                "side": "SELL",
                "type": "LIMIT",
                "quantity": f"{quantity:.3f}",
                "price": f"{take_profit_price:.2f}",
                "timeInForce": "GTC",
                "reduceOnly": "true",
            }
            tp_order = await self._post_signed_order("/fapi/v1/order", tp_params)

            sl_params = {
                "symbol": symbol,
                "side": "SELL",
                "type": "STOP_MARKET",
                "quantity": f"{quantity:.3f}",
                "stopPrice": f"{stop_loss_price:.2f}",
                "reduceOnly": "true",
                "priceProtect": "true",
            }
            sl_order = await self._post_signed_order("/fapi/v1/order", sl_params)

            if tp_order and sl_order:
                self.logger.info(
                    f"Successfully placed TP (ID: {tp_order['orderId']}) and SL (ID: {sl_order['orderId']})"
                )
                return {"tp_order": tp_order, "sl_order": sl_order}
            else:
                raise Exception("One or both OCO orders failed to place.")

        except Exception as e:
            self.logger.error(f"CRITICAL: OCO Order placement failed for {symbol}: {e}.")
            return None

    async def check_order_status(self, symbol: str, order_id: str) -> Optional[Dict]:
        
        now = time.monotonic()
        if now < self.ban_expiry:
            wait_time = self.ban_expiry - now
            self.logger.error(
                f"[CIRCUIT BREAKER] Tripped! Halting all new requests for {wait_time:.1f}s."
            )
            await asyncio.sleep(wait_time)
            self.logger.info("[CIRCUIT BREAKER] Reset. Resuming requests.")

        params = {"symbol": symbol, "orderId": order_id}
        await self.limiter.acquire(1)
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._create_signature(params)
        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"{self.config['fapi']}/fapi/v1/order"
        try:
            async with self.session.get(
                url, headers=headers, params=params, timeout=20
            ) as response:
                
                if response.status in [429, 418]:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    ban_duration = retry_after + 5
                    self.ban_expiry = time.monotonic() + ban_duration
                    self.logger.error(
                        f"[CIRCUIT BREAKER] {response.status} Error. Tripping circuit for {ban_duration}s."
                    )
                    await asyncio.sleep(retry_after + 1)

                response.raise_for_status()
                return await response.json()
        except Exception as e:
            self.logger.error(f"Failed to check order status for {order_id}: {e}")
            return None

    async def cancel_order(self, symbol: str, order_id: str) -> Optional[Dict]:
        
        now = time.monotonic()
        if now < self.ban_expiry:
            wait_time = self.ban_expiry - now
            self.logger.error(
                f"[CIRCUIT BREAKER] Tripped! Halting all new requests for {wait_time:.1f}s."
            )
            await asyncio.sleep(wait_time)
            self.logger.info("[CIRCUIT BREAKER] Reset. Resuming requests.")

        params = {"symbol": symbol, "orderId": order_id}
        await self.limiter.acquire(1)
        params["timestamp"] = int(time.time() * 1000)
        params["signature"] = self._create_signature(params)
        headers = {"X-MBX-APIKEY": self.api_key}
        url = f"{self.config['fapi']}/fapi/v1/order"
        try:
            async with self.session.delete(
                url, headers=headers, data=params, timeout=20
            ) as response:

                if response.status in [429, 418]:
                    retry_after = int(response.headers.get("Retry-After", "60"))
                    ban_duration = retry_after + 5
                    self.ban_expiry = time.monotonic() + ban_duration
                    self.logger.error(
                        f"[CIRCUIT BREAKER] {response.status} Error. Tripping circuit for {ban_duration}s."
                    )
                    await asyncio.sleep(retry_after + 1)

                response.raise_for_status()
                return await response.json()
        except Exception as e:
            self.logger.error(f"Failed to cancel order {order_id}: {e}")
            return None


# --- CORE LOGIC & REPLAY ENGINE (Now a Hybrid L1/L2 Engine) ---
class StrategyEngine:
    def __init__(self, mgr: "BinanceManager", ws_manager: "WebSocketDataManager"):
        self.mgr = mgr
        # --- NEW: WS Manager is now a dependency ---
        self.ws_manager = ws_manager

    def _generate_event_id(self, symbol: str, event_time: pd.Timestamp) -> str:
        data = {
            "symbol": symbol,
            "timestamp": event_time.isoformat(),
            "strategy": "4h_pump_v6_oi_veto", # New strategy version
        }
        canonical_json = rfc8785.dumps(data)
        return hashlib.sha256(canonical_json).hexdigest()

    async def run_universe_curation(self, symbols: List[str]) -> List[str]:
        # This function is unchanged, it still curates the initial universe
        logging.info("Fetching bulk data for curation...")
        try:
            tickers_fut, tickers_spot, books, funding_rates = await asyncio.gather(
                self.mgr.get_all_tickers_24h(is_f=True),
                self.mgr.get_all_tickers_24h(is_f=False),
                self.mgr.get_all_book_tickers(),
                self.mgr.get_funding_rates(),
            )
        except Exception as e:
            logging.error(
                f"Failed to fetch bulk data for curation: {e}. Aborting cycle."
            )
            return []

        fut_tickers = {t["symbol"]: t for t in tickers_fut}
        spot_tickers = {t["symbol"]: t for t in tickers_spot}
        book_tickers = {b["symbol"]: b for b in books}
        funding_rates_map = {
            fr["symbol"]: float(fr["lastFundingRate"])
            for fr in funding_rates
            if "lastFundingRate" in fr
        }

        curated = []
        logging.info("Applying curation filters (Volume, Spread, Funding, Trend)...")
        
        htf_trend_tasks = {}
        for symbol in symbols:
            fut_ticker = fut_tickers.get(symbol)
            spot_ticker = spot_tickers.get(symbol)
            book_ticker = book_tickers.get(symbol)

            if not all([fut_ticker, spot_ticker, book_ticker]):
                continue
            if float(fut_ticker["quoteVolume"]) < MIN_24H_NOTIONAL_VOLUME:
                continue
            bid_price = float(book_ticker["bidPrice"])
            ask_price = float(book_ticker["askPrice"])
            if bid_price == 0:
                continue
            spread = (ask_price / bid_price - 1) * 100
            if spread > MAX_BID_ASK_SPREAD_PCT:
                continue

            funding_rate = funding_rates_map.get(symbol)
            if funding_rate is None or funding_rate > L1_MAX_FUNDING_RATE:
                continue
            
            htf_trend_tasks[symbol] = asyncio.create_task(
                self.mgr.get_klines(symbol, "4h", L1_HTF_TREND_PERIOD + 1, is_f=True)
            )

        logging.info(f"Checking HTF Trend for {len(htf_trend_tasks)} symbols...")
        htf_results = await asyncio.gather(*htf_trend_tasks.values(), return_exceptions=True)
        
        for symbol, result in zip(htf_trend_tasks.keys(), htf_results):
            if isinstance(result, Exception) or not result or len(result) < L1_HTF_TREND_PERIOD + 1:
                if isinstance(result, Exception):
                    logging.debug(f"HTF Trend check failed for {symbol}: {result}")
                continue
                
            try:
                df_4h = pd.DataFrame(
                    result, columns=["ts", "o", "h", "l", "c", "v", "ct", "qv", "tr", "tbav", "tbqv", "ig"]
                )
                df_4h["c"] = pd.to_numeric(df_4h["c"])
                df_4h.ta.ema(length=L1_HTF_TREND_PERIOD, close='c', append=True)
                
                ema_col_name = f"EMA_{L1_HTF_TREND_PERIOD}"
                if df_4h["c"].iloc[-1] > df_4h[ema_col_name].iloc[-1]:
                    curated.append(symbol)
            except Exception as e:
                logging.debug(f"Error calculating HTF trend for {symbol}: {e}")
                continue

        return curated

    # --- NEW: L1 Historical Scan ---
    async def run_l1_historical_scan(self, symbol: str):
        """
        Runs the L1 historical checks (Spot Accumulation) and saves
        to the DB if it passes. L2 checks are now LIVE.
        """
        try:
            # Only fetch historical data needed for L1
            kl_spot_d = await self.mgr.get_klines(symbol, "1d", L1_LOOKBACK_DAYS, is_f=False)
            
            if not kl_spot_d:
                return

            # --- This is the "fake" CVD, but we are only using it for a 
            # long-term L1 spot accumulation check, which is fine. ---
            deltas = [
                (float(k[5]) if float(k[4]) > float(k[1]) else -float(k[5])) for k in kl_spot_d
            ]
            c_deltas = np.cumsum(deltas)
            spot_accum_pct = (
                ((c_deltas[-1] / c_deltas[0]) - 1) * 100
                if len(c_deltas) > 1 and c_deltas[0] != 0
                else 0.0
            )
            # --- End "fake" CVD ---

            if spot_accum_pct >= L1_MIN_SPOT_ACCUMULATION_PCT:
                metrics = {
                    "l1_accum_pct": round(spot_accum_pct, 1)
                }
                await DBManager.write_l1_pass(symbol, spot_accum_pct, str(metrics))
        
        except Exception as e:
            logging.debug(f"Error in L1 historical scan for {symbol}: {e}")

    # --- NEW: L2 Live Confidence Score (v6.3.2) ---
    async def get_l2_confidence_score(
        self, symbol: str, l1_pass_data: aiosqlite.Row
    ) -> Tuple[Optional[float], Dict]:
        """
        Calculates the L2 confidence score using LIVE data
        from the WebSocket manager.
        """
        try:
            # 1. Get LIVE L2 data from WS Manager
            ws_data = await self.ws_manager.get_l2_snapshot_data(symbol)
            
            # Fetch 5m klines and OI history (still needed for BB/OI trend)
            kl_spot_5m, oi_hist_5m = await asyncio.gather(
                self.mgr.get_klines(
                    symbol, "5m", L2_MOMENTUM_LOOKBACK, is_f=False
                ),
                self.mgr.get_oi_history(
                    symbol, "5m", L2_MOMENTUM_LOOKBACK
                ),
            )

            if not ws_data or not kl_spot_5m or not oi_hist_5m:
                return None, {}

            # 2. Calculate Spot CVD Score (from REAL CVD)
            # This is the sum of the last 30 5-minute CVD bins
            real_cvd_5m_trend = ws_data.get("cvd_5m_trend", 0.0)
            # Normalize score: max(2.5 * (CVD trend), 0), capped at 50
            spot_cvd_score = min(max(real_cvd_5m_trend * 2.5, 0), 50) 

            # 3. Calculate OI Score (from REST data)
            df_oi_5m = pd.DataFrame(oi_hist_5m)
            oi_score = 0
            oi_trend = 0
            if not df_oi_5m.empty and len(df_oi_5m) > L2_OI_TREND_PERIOD:
                df_oi_5m["sumOpenInterest"] = pd.to_numeric(df_oi_5m["sumOpenInterest"])
                df_oi_5m["oi_delta"] = df_oi_5m["sumOpenInterest"].diff()
                df_oi_5m["oi_trend"] = ta.sma(
                    df_oi_5m["oi_delta"], length=L2_OI_TREND_PERIOD
                )
                
                if not df_oi_5m["oi_trend"].isna().all():
                    oi_trend = df_oi_5m["oi_trend"].iloc[-1]
                    oi_trend_prev = df_oi_5m["oi_trend"].iloc[-2]
                    
                    # --- 🚨 START OF UPGRADE V6.3.2 (RVN Trap Fix) ---
                    # HARD VETO for Spot-Perp Decoupling (The RVN Trap)
                    # If OI is flat or dumping, it's a squeeze, not an inflow.
                    if oi_trend <= 0:
                        metrics = {
                            "l1_accum_pct": round(l1_pass_data["l1_score"], 1), 
                            "veto_reason": "OI_DUMP",
                            "oi_trend": round(oi_trend, 0),
                        }
                        # Return None to fail L2 check and prevent signal
                        return None, metrics 
                    # --- 🚨 END OF UPGRADE V6.3.2 ---

                    if oi_trend > 0 and oi_trend > oi_trend_prev:
                        oi_score = 25
                    elif oi_trend > 0:
                        oi_score = 15
            
            # 4. Calculate BBands Score (from REST data)
            df_spot_5m = pd.DataFrame(
                kl_spot_5m,
                columns=[
                    "ts", "o", "h", "l", "c", "v", "ct", "qv", "tr", "tbav", "tbqv", "ig"
                ],
            )
            df_spot_5m[["o", "h", "l", "c"]] = df_spot_5m[["o", "h", "l", "c"]].apply(pd.to_numeric)
            df_spot_5m.ta.bbands(length=20, close='c', append=True)
            
            bb_score = 0
            bb_expansion = 0.0 # <-- FIX: Was 0 (int), now 0.0 (float)
            if "BBM_20_2.0" in df_spot_5m.columns and not df_spot_5m["BBM_20_2.0"].isna().all():
                bbm = df_spot_5m["BBM_20_2.0"].iloc[-1]
                if bbm > 0:
                    df_spot_5m["bb_expansion"] = (
                        df_spot_5m["BBU_20_2.0"] - df_spot_5m["BBL_20_2.0"]
                    ) / bbm
                    bb_expansion = df_spot_5m["bb_expansion"].iloc[-1]
                    if bb_expansion > L2_BB_EXPANSION_THRESHOLD:
                        bb_score = 25

            # Final Score
            confidence_score = spot_cvd_score + oi_score + bb_score

            metrics = {
                "l1_accum_pct": round(l1_pass_data["l1_score"], 1),
                "l2_real_cvd_trend": round(real_cvd_5m_trend, 1),
                "oi_trend": round(oi_trend, 0),
                "bb_exp_pct": round(bb_expansion * 100, 1),
                "conf_score": round(confidence_score, 1),
            }

            return confidence_score, metrics

        except Exception as e:
            logging.debug(f"Error calculating L2 score for {symbol}: {e}")
            return None, {}


# --- TRADE MANAGER (Unchanged) ---
class TradeManager:
    def __init__(
        self,
        mgr: "BinanceManager",
        ws_manager: "WebSocketDataManager",
        portfolio_risk_pct: float,
        reward_ratio: float,
        live_trading: bool,
    ):
        self.mgr = mgr
        self.ws_manager = ws_manager
        self.PORTFOLIO_RISK_PCT = portfolio_risk_pct / 100.0
        self.LIVE_TRADING = live_trading

        log_msg = (
            f"TradeManager initialized: Risk={portfolio_risk_pct}%, Veto R:R={VETO_MIN_TP1_RR}"
        )
        if not self.LIVE_TRADING:
            log_msg += " -- [SIMULATION MODE ACTIVE. NO REAL TRADES WILL BE PLACED.] --"
            logging.warning(log_msg)
        else:
            log_msg += " -- [LIVE TRADING ACTIVE. REAL TRADES WILL BE PLACED.] --"
            logging.info(log_msg)

    async def get_atr_and_vol(self, symbol: str) -> Optional[Tuple[float, float]]:
        try:
            kl_4h = await self.mgr.get_klines(symbol, "4h", 15, is_f=True)
            if not kl_4h or len(kl_4h) < 15:
                return None
            df_4h = pd.DataFrame(
                kl_4h,
                columns=[
                    "ts", "o", "h", "l", "c", "v", "ct", "qv", "tr", "tbav", "tbqv", "ig"
                ],
            )
            df_4h[["h", "l", "c"]] = df_4h[["h", "l", "c"]].apply(pd.to_numeric)
            df_4h["tr"] = np.maximum(
                df_4h["h"] - df_4h["l"],
                np.maximum(
                    abs(df_4h["h"] - df_4h["c"].shift()),
                    abs(df_4h["l"] - df_4h["c"].shift()),
                ),
            )
            atr = df_4h["tr"].rolling(window=14).mean().iloc[-1]

            kl_5m = await self.mgr.get_klines(
                symbol, "5m", VETO_LIQUIDITY_CAP_BARS, is_f=True
            )
            if not kl_5m or len(kl_5m) < VETO_LIQUIDITY_CAP_BARS:
                return None
            df_5m = pd.DataFrame(
                kl_5m,
                columns=[
                    "ts", "o", "h", "l", "c", "v", "ct", "qv", "tr", "tbav", "tbqv", "ig"
                ],
            )
            df_5m["v"] = pd.to_numeric(df_5m["v"])
            avg_5m_vol = df_5m["v"].mean()

            if atr is None or np.isnan(atr) or avg_5m_vol is None or avg_5m_vol == 0:
                return None

            return atr, avg_5m_vol
        except Exception as e:
            logging.error(f"Failed to calculate ATR/Vol for {symbol}: {e}")
            return None

    async def execute_trade_entry(self, event_id: str, symbol: str):
        try:
            event = await DBManager.get_event_by_id(event_id)
            if not event:
                logging.error(
                    f"Could not find event {event_id} for trade execution."
                )
                return
            confidence_score = event["l2_score"]

            atr_vol_tuple = await self.get_atr_and_vol(symbol)
            all_tickers = await self.mgr.get_all_book_tickers()
            ticker = next((t for t in all_tickers if t["symbol"] == symbol), None)

            if not ticker:
                logging.warning(f"Skipping trade for {symbol}: Could not fetch ticker.")
                return
            if not atr_vol_tuple:
                logging.warning(
                    f"Skipping trade for {symbol}: Could not calculate ATR/Vol."
                )
                return

            atr, avg_5m_vol = atr_vol_tuple
            entry_price = float(ticker["askPrice"])
            if entry_price == 0:
                logging.warning(f"Skipping trade for {symbol}: Invalid entry price 0.")
                return

            risk_per_coin = 2 * atr

            risk_multiplier = 1.0
            if confidence_score > PROB_CONFIDENCE_THRESHOLD:
                risk_multiplier = PROB_UPSCALE_MULTIPLIER
                logging.info(
                    f"High confidence ({confidence_score:.0f} > {PROB_CONFIDENCE_THRESHOLD}). Applying {risk_multiplier}x risk."
                )

            account_balance = 10000.0
            risk_per_trade_usd = (
                account_balance * self.PORTFOLIO_RISK_PCT
            ) * risk_multiplier
            position_size = round(risk_per_trade_usd / risk_per_coin, 3)

            max_allowed_size = avg_5m_vol * VETO_LIQUIDITY_CAP_PCT
            if position_size > max_allowed_size:
                logging.warning(
                    f"TRADE VETO (LIQUIDITY): Position size {position_size} > {VETO_LIQUIDITY_CAP_PCT*100}% of {VETO_LIQUIDITY_CAP_BARS}-bar avg vol ({max_allowed_size:.3f})."
                )
                await DBManager.update_status(event_id, "VETO_LIQUIDITY")
                return

            sl_initial = entry_price - risk_per_coin
            tp1_initial = entry_price + (risk_per_coin * VETO_MIN_TP1_RR)
            tp2_initial = entry_price + (risk_per_coin * (VETO_MIN_TP1_RR + 1.0))
            tp3_initial = entry_price + (risk_per_coin * (VETO_MIN_TP1_RR + 2.0))

            liquidity_clusters = await self.ws_manager.find_liquidity_clusters(
                symbol, risk_per_trade_usd
            )

            sl_final = sl_initial
            tp_final = tp3_initial

            if liquidity_clusters and liquidity_clusters["bid_cluster"]:
                bid_cluster_price = liquidity_clusters["bid_cluster"]
                if sl_initial > bid_cluster_price and bid_cluster_price > 0:
                    sl_final = bid_cluster_price - (risk_per_coin * 0.05)
                    logging.info(
                        f"SL adjusted from {sl_initial:.4f} to {sl_final:.4f} (Beyond Bid Cluster)."
                    )

            final_risk_per_coin = entry_price - sl_final
            final_reward_to_tp1 = tp1_initial - entry_price

            if final_risk_per_coin <= 0:
                logging.warning(
                    f"TRADE VETO (INVALID SL): Final SL {sl_final:.4f} is above entry {entry_price:.4f}."
                )
                await DBManager.update_status(event_id, "VETO_SL")
                return

            final_rr_to_tp1 = final_reward_to_tp1 / final_risk_per_coin

            if final_rr_to_tp1 < VETO_MIN_TP1_RR:
                logging.warning(
                    f"TRADE VETO (R:R): Final R:R to TP1 ({final_rr_to_tp1:.2f}) is < {VETO_MIN_TP1_RR} after SL adjustment."
                )
                await DBManager.update_status(event_id, "VETO_RR")
                return

            final_sl = round(sl_final, 4)
            final_tp = round(tp3_initial, 4)

            if not self.LIVE_TRADING:
                logging.info(f"--- [PAPER TRADE] ---")
                logging.info(f"Symbol: {symbol} (Confidence: {confidence_score:.0f})")
                logging.info(
                    f"Entry: {entry_price}, Size: {position_size} (Risk Multi: {risk_multiplier}x)"
                )
                logging.info(
                    f"Risk/ATR: {risk_per_coin:.4f} | R:R Target: 1:{VETO_MIN_TP1_RR}"
                )
                logging.info(
                    f"SL (Adj): {final_sl:.4f} | TP1: {tp1_initial:.4f} | TP2: {tp2_initial:.4f} | TP3: {final_tp:.4f}"
                )
                if liquidity_clusters:
                    logging.info(
                        f"Liquidity: Bid Cluster @ {liquidity_clusters.get('bid_cluster', 'N/A')}, Ask Cluster @ {liquidity_clusters.get('ask_cluster', 'N/A')}"
                    )
                logging.info(f"----------------------")

                actual_entry_price = entry_price
                tp_order_id = f"sim_tp_{int(time.time())}"
                sl_order_id = f"sim_sl_{int(time.time())}"

            else:
                entry_order = await self.mgr.place_market_order(
                    symbol, "BUY", position_size
                )
                if not entry_order or entry_order.get("status") != "FILLED":
                    logging.error(f"Entry order failed for {symbol}: {entry_order}")
                    return

                actual_entry_price = float(entry_order["avgPrice"])
                logging.info(
                    f"LIVE ENTRY FILLED: {position_size} {symbol} @ {actual_entry_price} (Conf: {confidence_score:.0f}, Multi: {risk_multiplier}x)"
                )

                oco_orders = await self.mgr.place_oco_order(
                    symbol=symbol,
                    quantity=position_size,
                    take_profit_price=final_tp,
                    stop_loss_price=final_sl,
                )

                if not oco_orders:
                    logging.critical(
                        f"OCO BRACKET FAILED for {symbol}. ATTEMPTING TO MARKET CLOSE."
                    )
                    await self.mgr.place_market_order(symbol, "SELL", position_size)
                    return

                tp_order_id = oco_orders["tp_order"]["orderId"]
                sl_order_id = oco_orders["sl_order"]["orderId"]

            await DBManager.open_new_trade(
                event_id,
                symbol,
                actual_entry_price,
                final_sl,
                tp1_initial,
                position_size,
                tp_order_id,
                sl_order_id,
            )
            await DBManager.update_status(event_id, "PROCESSED")

        except Exception as e:
            logging.error(
                f"Error during trade execution for {symbol}: {e}", exc_info=True
            )


# --- MAIN APPLICATION (v6.3 - Modified Logic) ---
class MainApp:
    def __init__(self, config):
        self.config = config
        self.mgr = None
        self.engine = None
        self.trader = None
        self.ws_manager = None
        self.live_trading = False
        self.trigger_logger = logging.getLogger("ws_triggers")
        self.trigger_logger.propagate = False
        self.trigger_handler = logging.StreamHandler(sys.stdout)
        self.trigger_handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
        )
        
        # --- NEW: Track L2 scores for ranking ---
        self.live_l2_scores = {}
        self.l1_pass_symbols = {}

    async def run(self):
        await DBManager.initialize()

        try:
            api_key = self.config.get("binance", "api_key")
            api_secret = self.config.get("binance", "api_secret")
            risk_pct = self.config.getfloat("settings", "portfolio_risk_pct")
            rr_ratio = self.config.getfloat("settings", "reward_ratio")
            self.live_trading = self.config.getboolean("settings", "live_trading")

        except Exception as e:
            logging.error(
                f"FATAL: Could not read 'config.ini'. Make sure it exists and has all keys. Error: {e}"
            )
            return

        async with aiohttp.ClientSession() as session:
            self.mgr = BinanceManager(session, api_key, api_secret)
            self.ws_manager = WebSocketDataManager()
            # --- MODIFIED: Pass WS Manager to Engine ---
            self.engine = StrategyEngine(self.mgr, self.ws_manager)

            self.trader = TradeManager(
                self.mgr, self.ws_manager, risk_pct, rr_ratio, self.live_trading
            )

            all_perps_info = await self.mgr.get_all_perp_info()
            all_symbols = [
                s["symbol"]
                for s in all_perps_info["symbols"]
                if s["quoteAsset"] == "USDT"
            ]

            while True:
                try:
                    start_time = time.time()

                    # --- Step 0: Curate Symbol Universe ---
                    logging.info("--- Step 0: Curating Symbol Universe ---")
                    curated_symbols = await self.engine.run_universe_curation(
                        all_symbols
                    )
                    logging.info(f"Found {len(curated_symbols)} suitable symbols.")

                    # --- Step 1: Update WS Manager with curated symbols ---
                    await self.ws_manager.update_symbols(curated_symbols)
                    await self.ws_manager.stop_connections()
                    await self.ws_manager.start_connections()

                    # --- Step 2: Run L1 Historical Scan ---
                    logging.info(
                        f"--- Step 1: Running L1 Historical Scan for {len(curated_symbols)} symbols ---"
                    )
                    l1_tasks = [
                        self.engine.run_l1_historical_scan(s) for s in curated_symbols
                    ]
                    await asyncio.gather(*l1_tasks)
                    
                    # Load L1 pass symbols into memory
                    l1_pass_rows = await DBManager.get_l1_pass_symbols()
                    self.l1_pass_symbols = {row["symbol"]: row for row in l1_pass_rows}
                    logging.info(f"Found {len(self.l1_pass_symbols)} symbols passing L1 filters.")

                    # --- Step 3: Run L2 Live Scoring & L3 Execution Check ---
                    logging.info(
                        f"--- Step 2 & 3: Running LIVE L2 Score & L3 Trigger Check ---"
                    )
                    all_trades_from_db = await DBManager.get_live_trades()
                    live_trades_from_db = [
                        t for t in all_trades_from_db if t["status"] == "LIVE"
                    ]
                    
                    # This now replaces the old replay loop
                    await self._check_and_process_live_signals(live_trades_from_db)

                    # --- Step 4: Monitor Live Trades ---
                    if hasattr(self, "live_trading") and (
                        self.live_trading
                        or (not self.live_trading and live_trades_from_db)
                    ):
                        await self._monitor_live_trades(live_trades_from_db)

                    # --- Step 5: Rank & Display ---
                    events_for_display = await DBManager.query_recent_events()
                    results = await self._query_and_rank_events(events_for_display)
                    self._display_results(results, curated_symbols, all_trades_from_db)

                    # --- Step 6: Wait ---
                    elapsed = time.time() - start_time
                    wait_time = max(0, SCAN_INTERVAL_SECONDS - elapsed)
                    logging.info(
                        f"Cycle finished in {elapsed:.2f}s. Waiting {wait_time:.2f}s."
                    )
                    await asyncio.sleep(wait_time)

                except KeyboardInterrupt:
                    await self.ws_manager.stop_connections()
                    break
                except Exception as e:
                    logging.error(f"Main loop error: {e}", exc_info=True)
                    await asyncio.sleep(60)

    # --- NEW: Replaced _check_and_process_executable_signals ---
    async def _check_and_process_live_signals(self, live_trades_from_db):
        live_symbols = {t["symbol"] for t in live_trades_from_db}
        
        # Check only symbols that passed L1 and are not in a live trade
        symbols_to_check = [
            symbol for symbol in self.l1_pass_symbols if symbol not in live_symbols
        ]
        
        tasks = []
        for symbol in symbols_to_check:
            tasks.append(self.process_single_symbol_live(symbol))
        
        await asyncio.gather(*tasks)

    # --- NEW: Logic for processing a single symbol's L2/L3 status ---
    async def process_single_symbol_live(self, symbol: str):
        try:
            l1_data = self.l1_pass_symbols[symbol]
            
            # 1. Get L2 Score
            confidence_score, metrics = await self.engine.get_l2_confidence_score(symbol, l1_data)
            
            if confidence_score is None:
                # This now catches the OI Veto
                veto_reason = metrics.get("veto_reason", "L2_CALC_ERROR")
                self.live_l2_scores[symbol] = {"score": 0, "status": veto_reason, "metrics": metrics}
                return

            self.live_l2_scores[symbol] = {"score": confidence_score, "status": "DETECTED", "metrics": metrics}

            # 2. Check if L2 passes
            if confidence_score < L2_MIN_CONFIDENCE_SCORE:
                return # L2 not high enough, don't check L3

            # 3. L2 Passed, check L3 Microstructure Trigger
            has_microstructure_trigger = await self.ws_manager.get_trigger_status(
                symbol
            )

            if has_microstructure_trigger:
                logging.info(
                    f"[TRADE] 🔥 L1/L2 CONFIDENCE SIGNAL + MICROSTRUCTURE TRIGGER for {symbol}. Handing to TradeManager."
                )
                
                # Create a new event in the DB
                event_time = datetime.now(timezone.utc)
                event_id = self.engine._generate_event_id(symbol, pd.to_datetime(event_time))
                
                await DBManager.write_event(
                    event_id,
                    symbol,
                    self.mgr.name,
                    event_time,
                    l1_data["l1_score"],
                    confidence_score,
                    str(metrics)
                )
                
                await DBManager.update_status(event_id, "EXECUTABLE")
                await self.trader.execute_trade_entry(event_id, symbol)

        except Exception as e:
            logging.error(f"Error processing live signal for {symbol}: {e}")

    async def _monitor_live_trades(self, live_trades):
        if not self.live_trading:
            return

        for trade in live_trades:
            try:
                if (
                    trade["tp_order_id"]
                    and not trade["tp_order_id"].startswith("sim_")
                    and trade["sl_order_id"]
                    and not trade["sl_order_id"].startswith("sim_")
                ):

                    tp_status = await self.mgr.check_order_status(
                        trade["symbol"], trade["tp_order_id"]
                    )
                    if tp_status and tp_status["status"] == "FILLED":
                        logging.info(
                            f"TAKE PROFIT HIT for {trade['symbol']}. Closing trade."
                        )
                        await DBManager.close_trade(trade["trade_id"], "TAKE_PROFIT")
                        logging.info(
                            f"Cancelling SL order {trade['sl_order_id']} for {trade['symbol']}."
                        )
                        sl_check = await self.mgr.check_order_status(
                            trade["symbol"], trade["sl_order_id"]
                        )
                        if sl_check and sl_check["status"] not in [
                            "FILLED",
                            "CANCELED",
                            "EXPIRED",
                        ]:
                            await self.mgr.cancel_order(
                                trade["symbol"], trade["sl_order_id"]
                            )
                        continue

                    sl_status = await self.mgr.check_order_status(
                        trade["symbol"], trade["sl_order_id"]
                    )
                    if sl_status and sl_status["status"] == "FILLED":
                        logging.info(
                            f"STOP LOSS HIT for {trade['symbol']}. Closing trade."
                        )
                        await DBManager.close_trade(trade["trade_id"], "STOP_LOSS")
                        logging.info(
                            f"Cancelling TP order {trade['tp_order_id']} for {trade['symbol']}."
                        )
                        tp_check = await self.mgr.check_order_status(
                            trade["symbol"], trade["tp_order_id"]
                        )
                        if tp_check and tp_check["status"] not in [
                            "FILLED",
                            "CANCELED",
                            "EXPIRED",
                        ]:
                            await self.mgr.cancel_order(
                                trade["symbol"], trade["tp_order_id"]
                            )
                        continue

            except Exception as e:
                logging.error(
                    f"Error monitoring trade {trade['trade_id']} ({trade['symbol']}): {e}"
                )

    async def _query_and_rank_events(self, events: List) -> List[Dict]:
        # This function now just ranks what's in the DB for display
        now = datetime.now(timezone.utc)
        ranked_results = []
        for event in events:
            event_time = datetime.fromtimestamp(event["event_time"], tz=timezone.utc)
            age_minutes = (now - event_time).total_seconds() / 60
            decay_factor = 0.5 ** (age_minutes / RECENCY_HALF_LIFE_MIN)
            
            final_score = event["l2_score"] * decay_factor

            ranked_results.append(
                {
                    "final_score": final_score,
                    "confidence": event["l2_score"],
                    "symbol": event["symbol"],
                    "exchange": event["exchange"],
                    "age_minutes": int(age_minutes),
                    "metrics": event["metrics"],
                    "status": event["status"],
                }
            )
        return sorted(ranked_results, key=lambda x: x["final_score"], reverse=True)

    def _display_results(
        self, results: List[Dict], curated_symbols: List[str], all_trades: List
    ):
        all_trades_map = {t["symbol"]: t for t in all_trades}
        live_trade_symbols = [t["symbol"] for t in all_trades if t["status"] == "LIVE"]

        # --- MODIFIED: Display from live L2 scores, not just DB events ---
        top_10_live = sorted(
            [
                {"symbol": s, **data}
                for s, data in self.live_l2_scores.items()
                if s not in live_trade_symbols
            ],
            key=lambda x: x.get("score", 0),
            reverse=True,
        )[:10]

        detected_symbols = [r["symbol"] for r in top_10_live if r.get("score", 0) >= L2_MIN_CONFIDENCE_SCORE]

        sim_tag = (
            f" {Colors.YELLOW}(SIMULATION MODE){Colors.END}"
            if not self.live_trading
            else f" {Colors.GREEN}(LIVE TRADING){Colors.END}"
        )

        summary_str = f"""
{Colors.BOLD}--- 🚀 4H Pump Bot Cycle Summary {sim_tag} ---{Colors.END}
🔭 {Colors.CYAN}Curated Universe:{Colors.END}   {len(curated_symbols)} Symbols (Filtered by Vol, Spread, Funding, 4h-Trend)
📊 {Colors.CYAN}L1 Pass Symbols:{Colors.END}    {len(self.l1_pass_symbols)} (Waiting for L2/L3)
{Colors.GREEN}🟢 LIVE TRADES:{Colors.END}       ({len(live_trade_symbols)}) {', '.join(live_trade_symbols[:5]) + ('...' if len(live_trade_symbols) > 5 else '')}
{Colors.YELLOW}⏳ WAITING FOR TRIGGER:{Colors.END}  ({len(detected_symbols)}) {', '.join(detected_symbols[:5]) + ('...' if len(detected_symbols) > 5 else '')}
"""
        print(summary_str)

        headers = [
            f"{Colors.BOLD}🏆 Rank{Colors.END}",
            f"{Colors.BOLD}🪙 Symbol{Colors.END}",
            f"{Colors.BOLD}🔥 Conf%{Colors.END}",
            f"{Colors.BOLD}🕒 Age (min){Colors.END}",
            f"{Colors.BOLD}📊 Status{Colors.END}",
            f"{Colors.BOLD}{Colors.GREEN}Entry{Colors.END}",
            f"{Colors.BOLD}{Colors.GREEN}TP1 ({VETO_MIN_TP1_RR}R) 🟢{Colors.END}",
            f"{Colors.BOLD}{Colors.GREEN}TP2 ({VETO_MIN_TP1_RR+1}R){Colors.END}",
            f"{Colors.BOLD}{Colors.GREEN}TP3 ({VETO_MIN_TP1_RR+2}R) 🍃{Colors.END}",
            f"{Colors.BOLD}{Colors.RED}SL 🛑{Colors.END}",
            f"{Colors.BOLD}Meta-Metrics{Colors.END}",
        ]

        table_data = []
        
        # Get recent trades for display
        display_events = {r['symbol']: r for r in results}
        
        for i, res in enumerate(top_10_live):
            symbol = res["symbol"]
            trade_data = all_trades_map.get(symbol)
            event_data = display_events.get(symbol)

            entry_str, sl_str, tp1_str, tp2_str, tp3_str = (
                "—", "—", "—", "—", "—",
            )
            meta_str = str(res.get("metrics", "{}"))
            age_str = "LIVE"
            
            tp1_color, tp2_color, tp3_color, sl_color = (
                tp1_str, tp2_str, tp3_str, sl_str,
            )

            if trade_data:
                # Symbol is in a trade
                entry = trade_data["entry_price"]
                sl = trade_data["stop_loss_price"]
                tp1 = trade_data["take_profit_price"]
                risk_per_coin = entry - sl
                tp2, tp3 = None, None
                if risk_per_coin > 0:
                    base_rr_tp1 = (tp1 - entry) / risk_per_coin
                    tp2 = entry + (risk_per_coin * (base_rr_tp1 + 1.0))
                    tp3 = entry + (risk_per_coin * (base_rr_tp1 + 2.0))
                    meta_str = f"R:R 1:{base_rr_tp1:.1f} OCO"
                entry_str = f"{entry:.4f}"
                sl_str = f"{sl:.4f}"
                tp1_str = f"{tp1:.4f}"
                tp2_str = f"{tp2:.4f}" if tp2 else "N/A"
                tp3_str = f"{tp3:.4f}" if tp3 else "N/A"
                tp1_color = f"{Colors.GREEN}{Colors.BOLD}{tp1_str}{Colors.END}"
                tp2_color = f"{Colors.GREEN}{tp2_str}{Colors.END}"
                tp3_color = f"{Colors.GREEN}{tp3_str}{Colors.END}"
                sl_color = f"{Colors.RED}{Colors.BOLD}{sl_str}{Colors.END}"
                
                if trade_data["status"] == "LIVE":
                    status_str = f"{Colors.GREEN}LIVE 🟢{Colors.END}"
                else:
                    status_str = f"{Colors.BLUE}CLOSED 🔵{Colors.END}"
                
                if event_data:
                    age_str = str(event_data['age_minutes'])

            else:
                # Symbol is detected but not in a trade
                if res.get("score", 0) >= L2_MIN_CONFIDENCE_SCORE:
                     status_str = f"{Colors.YELLOW}DETECTED ⏳{Colors.END}"
                elif res.get("status") == "OI_DUMP":
                     status_str = f"{Colors.RED}VETO (OI) 🛑{Colors.END}"
                else:
                     status_str = f"{Colors.BLUE}LOW_SCORE{Colors.END}"

                if event_data:
                    if event_data["status"].startswith("VETO_"):
                         status_str = f"{Colors.RED}{event_data['status']} 🛑{Colors.END}"
                    age_str = str(event_data['age_minutes'])
            
            score = res.get("score", 0)
            score_str = (
                f"{Colors.MAGENTA}{Colors.BOLD}{score:.0f}%{Colors.END}"
                if score > PROB_CONFIDENCE_THRESHOLD
                else f"{score:.0f}%"
            )

            table_data.append(
                [
                    i + 1,
                    f"{Colors.CYAN}{symbol}{Colors.END}",
                    score_str,
                    f"{Colors.GREEN}{age_str}{Colors.END}",
                    status_str,
                    entry_str,
                    tp1_color,
                    tp2_color,
                    tp3_color,
                    sl_color,
                    meta_str,
                ]
            )

        print(tabulate(table_data, headers=headers, tablefmt="grid"))
        print("\n" * 3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Robust 4-Hour Pump Screener & Bot")
    args = parser.parse_args()

    config = configparser.ConfigParser()
    if not config.read("config.ini"):
        logging.error("FATAL: 'config.ini' file not found. Please create it.")
        sys.exit(1)

    # import os <-- This is now moved up
    # os.makedirs("data", exist_ok=True) <-- This is now moved up
    # os.makedirs("logs", exist_ok=True) <-- This is now moved up

    app = MainApp(config)
    try:
        asyncio.run(app.run())
    except KeyboardInterrupt:
        print(f"\n{Colors.BOLD}Application terminated.{Colors.END}")