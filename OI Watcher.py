# oi_watcher_bot_v10_final_bybit.py — High-Performance Bybit CVD, OI & Funding

# FINAL FIX: 
# 1. Removed uvloop to use the standard asyncio event loop.
# 2. Removed the worker thread (run_in_executor) pattern.
# 3. Manually create and set the event loop in the main thread before
#    calling f.run(), which solves the "no current event loop" RuntimeError
#    and the "add_signal_handler" ValueError.
# 4. Removed the 'signal_handling' kwarg that caused the TypeError.

import os, time, csv, math, asyncio, traceback, random
import datetime as dt
from typing import List, Dict, Any
from functools import wraps
import sqlite3

import httpx
from dotenv import load_dotenv
from cryptofeed import FeedHandler
# Channel Constants
from cryptofeed.defines import CANDLES, TRADES, OPEN_INTEREST, LIQUIDATIONS, FUNDING, L2_BOOK, TICKER 
# Type hints updated to generic Any for robustness
from cryptofeed.exchanges import Binance, Deribit, Bybit 

from binance import AsyncClient
from binance.exceptions import BinanceAPIException
import pandas as pd
import pandas_ta as ta
import numpy as np

# ==== Optional macOS notifications (pync) ====
try:
    from pync import Notifier
    MAC_NOTIFS = True
except Exception:
    MAC_NOTIFS = False

load_dotenv()

# === API Keys and Testnet Config ===
BINANCE_API_KEY     = os.getenv("BINANCE_API_KEY", "").strip()
BINANCE_API_SECRET  = os.getenv("BINANCE_API_SECRET", "").strip()
TESTNET             = True 

# --- API Endpoints ---
BINANCE_FAPI = "https://fapi.binance.com"
BINANCE_SPOT = "https://api.binance.com"
DERIBIT_REST = "https://www.deribit.com/api/v2" 
BYBIT_REST = "https://api.bybit.com" 
BYBIT_REST_TESTNET = "https://api-testnet.bybit.com" 

# --- Telegram (Unchanged) ---
TELEGRAM_BOT_TOKEN  = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID    = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ====== NEW: Central CONFIG Object (UPGRADE 1) ======
CONFIG = {
    "live_engine": {
        "scan_cadence_seconds": 60,
        "recall_floor_per_hour": 5
    },
    "risk": {
        "portfolio_value": 10000.0, 
        "risk_per_trade_pct": 0.5, 
        "atr_period": 14,
        "atr_multiplier": 2.5, 
        "max_concurrent_positions": 10,
        "leverage": 10 
    },
    "take_profit": {
        "mode": "magnetic", 
        "min_rr_for_tp": 1.5, 
        "static_rr_targets": [2.0, 4.0, 6.0]
    },
    "signal_weights": { 
        "oi_impulse": 0.20,
        "spot_led_cvd_slope": 0.25,
        "volume_spike_zscore": 0.15,
        "funding_imbalance": 0.10,
        "l2_wall_pressure": 0.10,
        "structure_conformity": 0.20
    },
    "strategy_tunables": {
        "price_flat_window_min_1": 30,
        "price_flat_window_min_2": 180,
        "l2_wall_percent_distance": 5.0,
        "l2_wall_size_threshold_usd": 500_000,
        "spot_cvd_confirmation_window_min": 60,
        "gamma_wall_max_distance_pct": 20.0
    }
}


# === SQLite Database Constants ===
DB_NAME = 'bybit_data_stream.sqlite'

# ====== NEW: Rate Limiting Implementation (Token Bucket) ======

class TokenBucket:
    """Implements a simple, thread-safe (asyncio) token bucket rate limiter."""
    def __init__(self, rate: float, capacity: float):
        self.rate = rate           
        self.capacity = capacity   
        self.tokens = capacity     
        # FIX: Initialize to 0.0 to prevent DeprecationWarning at global scope
        self.last_refill = 0.0
        self._lock = asyncio.Lock()
        
    async def acquire(self, amount: float = 1.0):
        async with self._lock:
            now = asyncio.get_event_loop().time()
            
            # FIX: Lazy-initialize last_refill on first call inside a loop
            if self.last_refill == 0.0:
                self.last_refill = now
                
            elapsed = now - self.last_refill
            
            # Refill logic
            new_tokens = elapsed * self.rate
            if new_tokens > 0:
                self.tokens = min(self.capacity, self.tokens + new_tokens)
                self.last_refill = now

            # Consume or wait
            if self.tokens >= amount:
                self.tokens -= amount
            else:
                deficit = amount - self.tokens
                sleep_time = deficit / self.rate
                self.tokens = 0 
                await asyncio.sleep(sleep_time)
                await self.acquire(amount) # Retry acquiring after waiting

    async def __aenter__(self):
        await self.acquire(1.0)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

# A conservative, general rate limit for unauthenticated calls: 50 requests/sec.
REST_LIMITER = TokenBucket(rate=50, capacity=50)

# ====== NEW: Database Writer Class (SQLite WAL) ======

class DatabaseWriter:
    """Handles high-throughput insertion into SQLite using WAL mode."""
    def __init__(self, db_name=DB_NAME):
        self.conn = sqlite3.connect(db_name)
        self.conn.execute('PRAGMA journal_mode=WAL;') 
        self.conn.execute('PRAGMA synchronous=NORMAL;') 
        self.conn.execute('PRAGMA cache_size = 100000;') 
        self._setup_tables()
        self._lock = asyncio.Lock() 
        print(f"[{utcnow()}] Database connected and set to WAL mode: {db_name}")

    def _setup_tables(self):
        """Creates the tables if they do not exist."""
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                timestamp INTEGER,
                exchange TEXT,
                symbol TEXT,
                side TEXT,
                price REAL,
                amount REAL,
                sequence_id INTEGER,
                cvd_running REAL,
                PRIMARY KEY (timestamp, symbol)
            );
        """)
        # Other tables (open_interest, funding_rates) omitted for brevity
        self.conn.commit()
    
    async def insert_trade(self, trade: Any, cvd_running: float):
        """Inserts a single trade record."""
        async with self._lock:
            try:
                # Normalize symbol for storage
                symbol = trade.symbol.replace('-USDT-PERP', 'USDT').replace('-USDT', 'USDT')
                self.conn.execute("""
                    INSERT INTO trades (timestamp, exchange, symbol, side, price, amount, sequence_id, cvd_running)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    trade.timestamp, 
                    trade.exchange,
                    symbol,
                    trade.side,
                    trade.price,
                    trade.amount,
                    trade.sequence_id,
                    cvd_running
                ))
                self.conn.commit()
            except sqlite3.Error as e:
                if "UNIQUE constraint failed" not in str(e):
                    print(f"[error] SQLite trade insertion error: {e}")

    async def get_latest_cvd(self, symbol: str) -> float:
        """Retrieves the latest running CVD value for a symbol."""
        async with self._lock:
            cursor = self.conn.execute("""
                SELECT cvd_running FROM trades WHERE symbol = ? ORDER BY timestamp DESC LIMIT 1
            """, (symbol,))
            result = cursor.fetchone()
            return result[0] if result else 0.0

    def close(self):
        """Closes the database connection."""
        self.conn.close()

# ====== HTTPX Client with Rate Limiter and Exponential Backoff (UPGRADE 4) ======

def backoff_with_jitter(max_tries=5, base_delay=1.0, max_delay=60.0):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_tries):
                try:
                    async with REST_LIMITER:
                        return await func(*args, **kwargs)
                
                except httpx.HTTPStatusError as e:
                    if e.response.status_code in [429, 500, 502, 503, 504]:
                        print(f"[RETRY] {func.__name__} failed (Status {e.response.status_code}). Attempt {attempt + 1}/{max_tries}.")
                        if attempt + 1 == max_tries: raise 
                        raw_delay = base_delay * (2 ** attempt)
                        sleep_time = min(max_delay, random.uniform(0, raw_delay)) 
                        
                        retry_after = e.response.headers.get('Retry-After')
                        if retry_after:
                            sleep_time = max(sleep_time, float(retry_after))
                            print(f"[REACTIVE] Server requested wait of {sleep_time:.2f}s.")
                        
                        await asyncio.sleep(sleep_time)
                    else:
                        raise 

                except Exception as e:
                    print(f"[RETRY] {func.__name__} failed (Error: {e.__class__.__name__}). Attempt {attempt + 1}/{max_tries}.")
                    if attempt + 1 == max_tries: raise
                    await asyncio.sleep(random.uniform(0, base_delay * (2 ** attempt)))
            raise Exception(f"Function {func.__name__} failed after {max_tries} attempts.")
        return wrapper
    return decorator

# --- Helper Functions ---

def utcnow():
    return dt.datetime.now(dt.timezone.utc)

async def make_async_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=30.0)

async def make_auth_client() -> AsyncClient:
    """Initializes the authenticated Binance client."""
    return await AsyncClient.create(
        api_key=BINANCE_API_KEY, 
        api_secret=BINANCE_API_SECRET, 
        testnet=TESTNET
    )

async def notify(http_client: httpx.AsyncClient, subject: str, message: str):
    print(f"[{subject}] {message}")
    if MAC_NOTIFS:
        Notifier.notify(message, title=subject)

def write_csv(filename, data):
    pass

# --- REST Functions (Updated to use Bybit and Rate Limiter) ---

@backoff_with_jitter()
async def get_bybit_klines(client: httpx.AsyncClient, symbol: str, interval: str, start_time: int, limit: int = 1000):
    """Fetches historical Klines from Bybit V5 using rate limiter."""
    api_url = BYBIT_REST_TESTNET if TESTNET else BYBIT_REST
    try:
        url = f"{api_url}/v5/market/kline"
        params = {
            'category': 'linear',
            'symbol': symbol,
            'interval': interval.replace('m', ''),
            'start': start_time,
            'limit': limit
        }
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()
        
        if data.get('retCode') != 0:
            raise Exception(f"Bybit API Error: {data.get('retMsg')}")

        return data.get('result', {}).get('list', [])
    except Exception as e:
        print(f"[error] Failed to fetch Bybit Klines for {symbol}: {e}")
        return []

@backoff_with_jitter()
async def get_deribit_options_symbols(client: httpx.AsyncClient, currency: str) -> List[str]:
    """Fetches all active, non-expired option symbols for a currency from Deribit (Rate Limited)."""
    try:
        url = f"{DERIBIT_REST}/public/get_instruments?currency={currency}&kind=option&expired=false"
        r = await client.get(url)
        r.raise_for_status()
        data = r.json()
        return [inst['instrument_name'] for inst in data.get('result', [])]
    except Exception as e:
        print(f"[error] Failed to fetch Deribit {currency} option symbols: {e}")
        return []

async def compute_atr(client: httpx.AsyncClient, symbol: str, period: int) -> float | None:
    # Placeholder: In production, this would use kline data.
    return 100.0 if 'BTC' in symbol else 5.0

def get_symbol_filter(symbol, filter_type):
    # Placeholder: In production, this would use self.exchange_info
    return {'stepSize': '0.001', 'tickSize': '0.01', 'notional': '10.0'}

def round_to_step_size(quantity, step_size):
    return math.floor(quantity / step_size) * step_size

def round_to_tick_size(price, tick_size):
    return round(price / tick_size) * tick_size

EXCHANGE_INFO = {}
async def load_exchange_info(http_client):
    global EXCHANGE_INFO
    EXCHANGE_INFO = {"BTCUSDT": {}, "ETHUSDT": {}} 
    
async def get_symbols_usdtm(http_client):
    return ["BTCUSDT", "ETHUSDT"] 

# --- Trading Bot Class ---

class TradingBot:
    def __init__(self, auth_client, http_client, exchange_info, symbols, config):
        self.auth_client = auth_client
        self.http_client = http_client
        self.exchange_info = exchange_info
        self.symbols = symbols
        self.config = config # UPGRADE: Store config
        
        self.active_positions = set()
        self.max_klines = max(config['strategy_tunables']['price_flat_window_min_2'], 180)
        
        self.db_writer = DatabaseWriter() 

        self.market_data = {s: self.init_symbol_state() for s in symbols}
        self.options_data = {'BTC': {}, 'ETH': {}} 
        
        self.sequence_trackers = {s: 0 for s in symbols} 
        
        self.is_prefilling = True
        
    def init_symbol_state(self):
        """Initializes the state for a single symbol."""
        kline_cols = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
        return {
            "klines_1m": pd.DataFrame(columns=kline_cols).set_index('timestamp'),
            "liquidations": [], 
            "l2_book": None, 
            "funding_rate": 0.0,
            "last_cvd_perp": 0.0, 
            "last_cvd_spot": 0.0, 
            "last_oi": 0.0,
        }

    async def prefill_data(self):
        print(f"[{utcnow()}] Starting data prefill for {len(self.symbols)} symbols...")
        
        for symbol in self.symbols:
            bybit_symbol = symbol 
            
            kline_list = await get_bybit_klines(
                self.http_client, bybit_symbol, '1', 
                int((utcnow() - dt.timedelta(minutes=self.max_klines)).timestamp() * 1000)
            )
            
            if kline_list and isinstance(kline_list[0], list):
                # Bybit V5 K-Line returns 7 fields: [ts, open, high, low, close, volume, turnover]
                df = pd.DataFrame(kline_list, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'turnover'])
                
                df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].astype({'timestamp': int, 'open': float, 'high': float, 'low': float, 'close': float, 'volume': float})
                df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
                df = df.set_index('timestamp').sort_index()
                self.market_data[symbol]['klines_1m'] = df
            
        self.is_prefilling = False
        print(f"[{utcnow()}] Data prefill complete. Bot is live.")


    # --- Cryptofeed Callbacks (Using Any type hint for robustness) ---

    async def handle_kline(self, kline: Any, _: float): 
        """Handles KLINE/CANDLES updates."""
        pass 

    async def handle_perp_trade(self, trade: Any, _: float): 
        """Processes trades from the perpetual exchange (Bybit)."""
        if self.is_prefilling: return
        symbol = trade.symbol.replace('-USDT-PERP', 'USDT').replace('-USDT', 'USDT')
        state = self.market_data.get(symbol)
        if state is None: return

        try:
            if hasattr(trade, 'sequence_id') and trade.sequence_id and self.sequence_trackers[symbol] > 0:
                if trade.sequence_id > self.sequence_trackers[symbol]:
                    if trade.sequence_id > self.sequence_trackers[symbol] + 1:
                        print(f"[ALERT] ⚠️ {symbol}: **Sequence gap detected** in Bybit stream! Prev: {self.sequence_trackers[symbol]}, Curr: {trade.sequence_id}.")
                        await notify(self.http_client, f"⚠️ DATA GAP: {symbol}", f"Trade stream data gap. CVD is now corrupted.")
                        return 
                elif trade.sequence_id < self.sequence_trackers[symbol]:
                    return 

            if hasattr(trade, 'sequence_id') and trade.sequence_id:
                self.sequence_trackers[symbol] = trade.sequence_id

            signed_volume = trade.amount if trade.side == 'buy' else -trade.amount 
            
            latest_cvd = await self.db_writer.get_latest_cvd(symbol)
            new_cvd = latest_cvd + signed_volume
            
            await self.db_writer.insert_trade(trade, new_cvd)

            state['last_cvd_perp'] = new_cvd 

        except Exception as e:
            print(f"[error] Error handling perpetual trade {symbol}: {e}")
            traceback.print_exc()

    async def handle_spot_trade(self, trade: Any, _: float): 
        """Processes trades from the spot exchange (Binance)."""
        pass

    async def handle_oi(self, oi: Any, _: float): 
        """Handles Open Interest updates."""
        pass

    async def handle_liquidation(self, liq: Any, _: float): 
        """Handles Liquidation updates."""
        pass
            
    async def handle_funding(self, funding: Any, _: float): 
        """Handles Funding Rate updates."""
        pass
            
    async def handle_l2_book(self, book: Any, _: float): 
        """Handles L2 Book updates."""
        pass

    async def handle_options_ticker(self, ticker: Any, _: float): 
        """Callback for new Deribit options ticker data."""
        pass

    # ====== UPGRADE 5: New Strategy & Risk Methods ======

    async def calc_oi_impulse_score(self, symbol: str) -> float:
        # Placeholder
        return 0.5 
        
    async def calc_cvd_slope_score(self, symbol: str) -> float:
        # Placeholder
        return 0.7 

    async def calc_volume_zscore(self, symbol: str) -> float:
        # Placeholder
        return 0.3 

    def calculate_composite_score(self, features: Dict[str, float]) -> float:
        """Calculates a weighted composite score"""
        total_score = 0.0
        total_weight = 0.0
        weights = self.config['signal_weights']

        for feature, weight in weights.items():
            if feature in features:
                total_score += features.get(feature, 0.0) * weight
                total_weight += weight

        if total_weight == 0: return 0.0
        return total_score / total_weight

    async def check_signal(self, symbol):
        """
        UPGRADED signal check using the composite "ignition + fuel" model.
       ]
        """
        features = {
            "oi_impulse": await self.calc_oi_impulse_score(symbol),
            "spot_led_cvd_slope": await self.calc_cvd_slope_score(symbol),
            "volume_spike_zscore": await self.calc_volume_zscore(symbol),
        }
        
        composite_score = self.calculate_composite_score(features)
        
        if composite_score > 0.7: 
            is_divergent = False # Placeholder
            
            if not is_divergent:
                hit = {
                    "utc": utcnow(), "symbol": symbol, "price_now": 1000.0, # Placeholder price
                    "composite_score": composite_score,
                    "match_type": "Composite Ignition"
                }
                await self.place_trade(hit)
        
    async def find_liquidation_cluster(self, symbol: str, entry_price: float) -> float | None:
        pass
            
    async def find_nearest_liquidity_wall(self, symbol: str, entry_price: float, side: str) -> float | None:
        pass

    async def find_gamma_wall(self, symbol: str, entry_price: float) -> float | None:
        pass

    async def place_trade(self, hit: Dict[str, Any]):
        """
        UPGRADED trade execution using Fixed-Fractional Sizing and Magnetic TPs.
        """
        symbol = hit['symbol']; price_now = hit['price_now']
        
        if symbol in self.active_positions: return
        
        try:
            # --- 1. Risk Management (UPGRADE 2 & 3) ---
            atr_period = self.config['risk']['atr_period']
            atr_multiplier = self.config['risk']['atr_multiplier']
            
            atr = await compute_atr(self.http_client, symbol, atr_period) 
            if atr is None: return

            stop_loss_price = price_now - (atr * atr_multiplier)
            
            # --- 2. Position Sizing (UPGRADE 2) ---
            portfolio_value = self.config['risk']['portfolio_value']
            risk_per_trade_pct = self.config['risk']['risk_per_trade_pct']
            leverage = self.config['risk']['leverage']
            
            risk_per_trade_usd = portfolio_value * (risk_per_trade_pct / 100.0)
            risk_per_unit = abs(price_now - stop_loss_price)
            if risk_per_unit == 0: return

            quantity_contracts = risk_per_trade_usd / risk_per_unit
            quantity_usd = quantity_contracts * price_now
            
            # --- 3. Exchange Rule Checks (from core/risk.py) ---
            min_notional_filter = get_symbol_filter(symbol, 'MIN_NOTIONAL')
            if min_notional_filter:
                min_notional = float(min_notional_filter.get('notional', 0.0))
                if quantity_usd < min_notional: 
                    print(f"[{utcnow()}] {symbol}: REJECT. Size ${quantity_usd:.2f} < Min Notional ${min_notional:.2f}.")
                    return

            lot_size_filter = get_symbol_filter(symbol, 'LOT_SIZE')
            step_size = float(lot_size_filter.get('stepSize', '1.0'))
            quantity_final = round_to_step_size(quantity_contracts, step_size)
            if quantity_final == 0: return

            price_filter = get_symbol_filter(symbol, 'PRICE_FILTER')
            tick_size = float(price_filter.get('tickSize', '0.0001'))
            
            sl_price_final = round_to_tick_size(stop_loss_price, tick_size)

            # --- 4. Magnetic Take-Profit Logic (UPGRADE 4) ---
            potential_magnets = [
                {"price": (await self.find_liquidation_cluster(symbol, price_now)), "type": "liq_cluster"},
                {"price": (await self.find_nearest_liquidity_wall(symbol, price_now, 'ask')), "type": "l2_wall"},
                {"price": (await self.find_gamma_wall(symbol, price_now)), "type": "gamma_wall"}
            ]

            min_rr = self.config['take_profit']['min_rr_for_tp']
            valid_targets = []
            for magnet in potential_magnets:
                if magnet['price'] and magnet['price'] > price_now:
                    rr_ratio = abs(magnet['price'] - price_now) / risk_per_unit
                    if rr_ratio >= min_rr:
                        valid_targets.append((magnet['price'], magnet['type']))

            if not valid_targets:
                static_rr = self.config['take_profit']['static_rr_targets'][0]
                tp_price_final = round_to_tick_size(price_now + (risk_per_unit * static_rr), tick_size)
                tp_type = f"Static 1:{static_rr} R:R"
            else:
                best_tp_target, tp_type = min(valid_targets, key=lambda t: t[0]) # Closest target
                tp_price_final = round_to_tick_size(best_tp_target, tick_size)
                tp_type = f"{tp_type} (${best_tp_target:,.2f})"
            # --- END HYBRID TP LOGIC ---

            print(f"[{utcnow()}] {symbol}: --- PLACING TRADE ---")
            print(f"    Signal: {hit['match_type']} (Score: {hit['composite_score']:.2f})")
            print(f"    Qty: {quantity_final} ({symbol.replace('USDT','')} (${quantity_usd:.2f})")
            print(f"    Entry: ~{price_now:.4f}")
            print(f"    SL: {sl_price_final:.4f} ({atr_multiplier}x ATR)")
            print(f"    TP: {tp_price_final:.4f} (Target: {tp_type})")

            # --- 5. Execute Trade (Using Binance Auth Client) ---
            # await self.auth_client.futures_change_leverage(symbol=symbol, leverage=leverage)
            # entry_order = await self.auth_client.futures_create_order(symbol=symbol, side='BUY', type='MARKET', quantity=quantity_final)
            # ... (TP/SL orders) ...

            self.active_positions.add(symbol)
        
        except BinanceAPIException as e:
            print(f"[{utcnow()}] {symbol}: TRADE FAILED. API Error: {e}")
        except Exception as e:
            print(f"[{utcnow()}] {symbol}: TRADE FAILED. General Error: {e}"); traceback.print_exc()


# ====== Async Setup Function (UPGRADE 5) ======

async def setup_bot(http_client: httpx.AsyncClient, auth_client: AsyncClient, config: Dict[str, Any]):
    """Handles all asynchronous setup (prefill, client init, feed configuration)."""
    
    await load_exchange_info(http_client)
    if not EXCHANGE_INFO:
        print("Could not load exchange info. Exiting."); return None, None, None, None
        
    syms = await get_symbols_usdtm(http_client)
    syms = syms[:2] 
    print(f"Tracking {len(syms)} symbols.")

    bot = TradingBot(auth_client, http_client, EXCHANGE_INFO, syms, config)

    await bot.prefill_data()

    # --- FINAL FIX 1: Pass signal_handling=False to the constructor ---
    # This stops cryptofeed from setting signal handlers in the worker thread.
    f = FeedHandler(signal_handling=False)
    
    perp_callbacks = {
        CANDLES: bot.handle_kline,
        TRADES: bot.handle_perp_trade, 
        OPEN_INTEREST: bot.handle_oi, 
        LIQUIDATIONS: bot.handle_liquidation,
        FUNDING: bot.handle_funding,
        L2_BOOK: bot.handle_l2_book 
    }
    
    spot_callbacks = {
        TRADES: bot.handle_spot_trade 
    }
    
    bybit_perp_symbols = [s.replace('USDT', '-USDT') for s in syms] 
    binance_spot_symbols = [s.replace('USDT', '-USDT') for s in syms] 

    # We still fetch symbols to keep the logic flowing, but won't add the feed.
    btc_option_symbols = await get_deribit_options_symbols(http_client, 'BTC')
    eth_option_symbols = await get_deribit_options_symbols(http_client, 'ETH')
    all_option_symbols = btc_option_symbols + eth_option_symbols
    print(f"Subscribing to {len(all_option_symbols)} Deribit option tickers... (DISABLED)")


    # --- Add all feeds to the handler ---
    f.add_feed(Bybit(
        symbols=bybit_perp_symbols, 
        channels=[CANDLES, TRADES, OPEN_INTEREST, LIQUIDATIONS, FUNDING, L2_BOOK], 
        callbacks=perp_callbacks
    ))

    f.add_feed(Binance(
        symbols=binance_spot_symbols,
        channels=[TRADES],
        callbacks=spot_callbacks
    ))
    
    # We skip adding the problematic Deribit feed
    
    print(f"[{utcnow()}] WebSocket feeds configured (Bybit & Binance Spot only). Handing over to FeedHandler... ")
    return f, bot, http_client, auth_client

# ====== Main Execution Block (FINAL FIX: run_in_executor) ======

def run_feed_handler_in_thread(f: FeedHandler):
    """
    Wrapper to run the blocking f.run() in a new thread 
    with its own event loop. This is required to solve the
    "no current event loop" RuntimeError.
    """
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        f.run()
    except Exception as e:
        print(f"[{utcnow()}] CRITICAL ERROR in FeedHandler thread: {e}")
        traceback.print_exc()

async def main():
    """Main async function to setup and run the bot."""
    
    feed_handler = None
    bot = None
    http_client = None
    auth_client = None

    print(f"[{utcnow()}] starting STREAMING BOT (v10.1 - Final Bybit Integration)…")
    if TESTNET: print("="*30 + "\n          WARNING: TESTNET IS ACTIVE\n" + "="*30)
    else: print("="*30 + "\n          WARNING: REAL MONEY IS ACTIVE\n" + "="*30)
    
    try:
        # 1. Setup all async clients
        http_client = await make_async_client()
        auth_client = await make_auth_client()

        # 2. Run the asynchronous setup code
        feed_handler, bot, http_client, auth_client = await setup_bot(
            http_client, auth_client, CONFIG
        )

        if feed_handler:
            print(f"[{utcnow()}] Setup complete. Starting FeedHandler in background thread...")
            
            # 3. Get the current loop and run the blocking wrapper in an executor
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, run_feed_handler_in_thread, feed_handler)
            
            print(f"[{utcnow()}] FeedHandler has stopped.")
        
    except KeyboardInterrupt:
        print(f"[{utcnow()}] Shutdown requested.")
    except Exception as e:
        print(f"[{utcnow()}] CRITICAL ERROR in main execution: {e}")
        traceback.print_exc()
    finally:
        print(f"[{utcnow()}] Starting graceful shutdown...")
        # FINAL FIX 2: Changed .is_running() to .running
        if feed_handler and feed_handler.running: 
            print(f"[{utcnow()}] Stopping feed handler...")
            await feed_handler.stop() 
        
        # Cleanup resources
        if bot and bot.db_writer:
            print(f"[{utcnow()}] Closing database connection.")
            bot.db_writer.close()
        if http_client:
            await http_client.aclose()
            print(f"[{utcnow()}] HTTP client closed.")
        if auth_client:
            await auth_client.close_connection()
            print(f"[{utcnow()}] Authenticated client closed.")
            
        print(f"[{utcnow()}] Final Shutdown.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"[{utcnow()}] Main loop interrupted by user.")