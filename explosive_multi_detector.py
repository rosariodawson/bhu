#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Explosive Screener — Guarded Single File
- Robust parsing: no "too many values to unpack" crashes
- Decorated detectors with safety returns (score, details)
- Minimal-REST policy + polite rate-limiter (token bucket + backoff)
- WebSocket-first (Binance, Bybit, Bitget public feeds)
- Single Rich Live table (non-jumping) for consolidated alerts
- Plug-in detectors: vpvr_gap, microcap_fresh, god_preignite (scaffold)

Tested on macOS Python 3.10–3.12. Requires: aiohttp, rich, orjson (optional).
> pip install aiohttp rich orjson
"""
import asyncio, time, math, json, random, traceback, contextlib
from collections import deque, defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple
try:
    import orjson as _json
    def jloads(b): return _json.loads(b)
    def jdumps(o): return _json.dumps(o)
except Exception:
    def jloads(b): return json.loads(b)
    def jdumps(o): return json.dumps(o).encode()

from rich.live import Live
from rich.table import Table
from rich.console import Console
from rich.panel import Panel
from rich import box

console = Console()

# ------------------------------ Rate Limiter (polite, ban-safe) ------------------------------
class TokenBucket:
    def __init__(self, capacity:int, refill_per_sec:float):
        self.capacity = capacity
        self.tokens = capacity
        self.refill = refill_per_sec
        self.last = time.time()

    async def take(self, n:int=1):
        while True:
            now = time.time()
            dt = now - self.last
            if dt > 0:
                self.tokens = min(self.capacity, self.tokens + dt*self.refill)
                self.last = now
            if self.tokens >= n:
                self.tokens -= n
                return
            await asyncio.sleep(max(0.05, (n - self.tokens)/max(self.refill,1e-6)))

class Backoff:
    def __init__(self, base=0.5, cap=30.0):
        self.base, self.cap, self.k = base, cap, 0
    async def wait(self, jitter=True):
        t = min(self.cap, self.base*(2**self.k))
        if jitter: t = t*(0.5+random.random()*0.5)
        await asyncio.sleep(t)
        self.k = min(self.k+1, 10)
    def reset(self): self.k = 0

# Shared REST limiter (only if we need REST; we try to avoid it)
REST_BUCKET = TokenBucket(capacity=10, refill_per_sec=5.0)  # ~10 tokens, ~5 r/s

# ------------------------------ Safe helpers (no-unpack-crash) ------------------------------
def pick_price_qty(level: Any) -> Tuple[Optional[float], Optional[float]]:
    """Accept dict or sequence of variable length → (price, qty) or (None, None)"""
    if level is None:
        return None, None
    if isinstance(level, dict):
        p = level.get("p") or level.get("price") or level.get("P") or level.get("0")
        q = level.get("q") or level.get("size")  or level.get("Q") or level.get("1") or level.get("amount")
        try:
            return (float(p), float(q)) if (p is not None and q is not None) else (None, None)
        except Exception:
            return (None, None)
    if isinstance(level, (list, tuple)):
        if len(level) >= 2:
            try:
                return float(level[0]), float(level[1])
            except Exception:
                return None, None
    return None, None

def split2(s: Any, sep=":") -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(s, str):
        return None, None
    parts = s.split(sep, 1)
    if len(parts) < 2:
        return parts[0] if parts else None, None
    return parts[0], parts[1]

def hours_since(ts: Any) -> Optional[float]:
    try:
        now = time.time()
        t = float(ts)
        h = (now - t)/3600.0
        if h < 0 or h > 24*365*20:  # clamp >20y
            return None
        return h
    except Exception:
        return None

def safe_float(x, default=None):
    try: return float(x)
    except Exception: return default

# ------------------------------ Detector guard ------------------------------
def feature_guard(fn):
    async def wrapped(*args, **kwargs):
        try:
            out = await fn(*args, **kwargs)
            # normalize to (score, details)
            if isinstance(out, tuple):
                if len(out) >= 2: return out[0], out[1]
                if len(out) == 1: return out[0], {}
            if isinstance(out, (int, float)): return float(out), {}
            if isinstance(out, dict): return 0.0, out
            return 0.0, {"_note":"normalized"}
        except Exception as e:
            if "too many values to unpack" in str(e):
                return 0.0, {"error":"unpack", "where":fn.__name__}
            # don't kill loop; log and continue
            console.log(f"[red][DETECTOR ERR][/red] {fn.__name__}: {e}")
            return 0.0, {"error":str(e), "where":fn.__name__}
    return wrapped

# ------------------------------ Minimal venues: public WS only ------------------------------
import aiohttp

class VenueWS:
    def __init__(self, name:str, url:str, subscribe_payload:Any, parser, heartbeat=20.0):
        self.name=name
        self.url=url
        self.subscribe_payload=subscribe_payload
        self.parser=parser
        self.heartbeat=heartbeat
        self._stop=False

    async def run(self, q:asyncio.Queue):
        backoff=Backoff()
        while not self._stop:
            try:
                async with aiohttp.ClientSession() as ses:
                    async with ses.ws_connect(self.url, heartbeat=self.heartbeat) as ws:
                        # send subscription
                        if self.subscribe_payload:
                            await ws.send_str(self.subscribe_payload if isinstance(self.subscribe_payload,str)
                                              else json.dumps(self.subscribe_payload))
                        console.log(f"[green][WS] {self.name} connected[/green]")
                        backoff.reset()
                        async for msg in ws:
                            if msg.type == aiohttp.WSMsgType.TEXT:
                                data = jloads(msg.data.encode()) if isinstance(msg.data,str) else jloads(msg.data)
                                for item in self.parser(data):
                                    # Push normalized (venue, symbol, kind, payload)
                                    await q.put(item)
                            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                                break
            except Exception as e:
                console.log(f"[yellow][WS] {self.name} error:[/yellow] {e}")
                await backoff.wait()
        console.log(f"[red][WS] {self.name} stopped[/red]")

# ------------------------------ Parsers (arr/agg trade styles) ------------------------------
def binance_aggtrade_arr_parser(msg:Dict[str,Any]):
    # expect {"stream":"!aggTrade@arr","data":[{...},{...}]}
    data = msg.get("data") or msg.get("stream") and msg  # some gateways reshape
    items = []
    arr = msg.get("data") if isinstance(msg.get("data"), list) else msg.get("data",[])
    if isinstance(arr, list):
        for tr in arr:
            sym = tr.get("s") or tr.get("symbol")
            p   = safe_float(tr.get("p"))
            q   = safe_float(tr.get("q"))
            T   = tr.get("T") or tr.get("E") or int(time.time()*1000)
            if sym and p and q:
                items.append(("binance", sym, "trade", {"p":p,"q":q,"t":T,"is_buyer_maker":tr.get("m")}))
    return items

def bybit_trade_parser(msg:Dict[str,Any]):
    # unified v5 public trade: {"topic":"trade.linear","data":[{"s":"BTCUSDT","p":"...","v":"...","TS":...,"S":"Buy"}]}
    items=[]
    d = msg.get("data")
    if isinstance(d, list):
        for tr in d:
            sym = tr.get("s") or tr.get("symbol")
            p = safe_float(tr.get("p"))
            q = safe_float(tr.get("v") or tr.get("q"))
            T = tr.get("TS") or tr.get("ts") or int(time.time()*1000)
            if sym and p and q:
                items.append(("bybit", sym, "trade", {"p":p,"q":q,"t":T,"side":tr.get("S")}))
    return items

def bitget_trade_parser(msg:Dict[str,Any]):
    # bitget public trades: {"event":"update","arg":{"channel":"trade","instId":"BTCUSDT"},
    # "data":[{"price":"...","size":"...","side":"buy","ts":"..."}]}
    items=[]
    data = msg.get("data")
    arg  = msg.get("arg",{})
    sym  = arg.get("instId")
    if isinstance(data, list):
        for tr in data:
            p = safe_float(tr.get("price"))
            q = safe_float(tr.get("size"))
            T = safe_float(tr.get("ts")) or time.time()*1000
            if sym and p and q:
                items.append(("bitget", sym, "trade", {"p":p,"q":q,"t":T,"side":tr.get("side")}))
    return items

# ------------------------------ Feature engines (patched) ------------------------------
class Tape:
    """Per-symbol tape of recent trades"""
    __slots__=("trades","qsum","vsum")
    def __init__(self, maxlen:int=3000):
        self.trades: deque = deque(maxlen=maxlen)
        self.qsum=0.0
        self.vsum=0.0

    def push(self, p:float, q:float, t:int):
        self.trades.append((p,q,t))
        # lazy sums recomputed on slice queries; keep simple to avoid overflows

    def window(self, ms:int) -> List[Tuple[float,float,int]]:
        now=int(time.time()*1000)
        out=[]
        for (p,q,t) in reversed(self.trades):
            if now - t <= ms:
                out.append((p,q,t))
            else:
                break
        return out

# Per venue+symbol state
STATE: Dict[Tuple[str,str], Dict[str,Any]] = defaultdict(dict)
ALERTS: deque = deque(maxlen=500)

# ------------------------------ Detectors ------------------------------
@feature_guard
async def god_preignite(venue:str, symbol:str, tape:Tape) -> Tuple[float,Dict[str,Any]]:
    """Generic pre-ignition: short-window trade-per-sec zscore + mean size z + thin-ask proxy (stub)
    Returns (score ∈ [0,1.2], details)"""
    w30 = tape.window(30_000)
    if len(w30) < 20:
        return 0.0, {"reason":"tape_thin"}
    # Trades per second (approx)
    tps = len(w30)/30.0
    sizes = [q for (_,q,_) in w30]
    mean_sz = sum(sizes)/max(len(sizes),1)
    # Simple rolling baselines (store on STATE)
    key=(venue,symbol)
    st=STATE[key]
    base = st.get("tps_base", 0.2)
    base = 0.98*base + 0.02*tps
    st["tps_base"]=base
    # crude z
    z = (tps - base)/max(0.5*base+0.1, 0.1)
    msb= st.get("ms_base", 1.0)
    msb= 0.98*msb + 0.02*mean_sz
    st["ms_base"]=msb
    ms_z= (mean_sz - msb)/max(0.5*msb+1e-6, 1e-6)
    # ask_thin_proxy (not using orderbook; just a placeholder scaling by z & ms_z)
    ask_thin_proxy = min(1.0, max(0.0, 0.25+0.15*z + 0.1*ms_z))
    sweep_runs = int(3 + 2*z + 1*ms_z)
    score = max(0.0, min(1.2, 0.4 + 0.12*z + 0.08*ms_z + 0.1*ask_thin_proxy))
    return score, {
        "tps_z_30s": round(z,2),
        "mean_size_z": round(ms_z,2),
        "ask_thin_proxy": round(ask_thin_proxy,2),
        "sweep_runs": max(0,sweep_runs)
    }

@feature_guard
async def vpvr_gap(venue:str, symbol:str, tape:Tape) -> Tuple[float,Dict[str,Any]]:
    """Volume Profile (trade-based) — detect local 'air pockets' above price (potential air-gap run)
    No unpack errors: we never assume pair sizes.
    """
    w5m = tape.window(5*60_000)
    if len(w5m) < 60:
        return 0.0, {"reason":"tape_thin"}
    # Bucket by price step (rough)
    prices=[p for (p,_,_) in w5m]
    pmin, pmax = min(prices), max(prices)
    step = max((pmax-pmin)/50.0, (pmin+pmax)/2000.0, 1e-6)
    buckets: Dict[int, float] = defaultdict(float)
    for (p,q,_) in w5m:
        if p is None or q is None: 
            continue
        bi = int((p - pmin)/step)
        buckets[bi]+=q
    # Find current bucket and next 3 higher with low volume sum (gap proxy)
    lastp = w5m[-1][0]
    curi = int((lastp - pmin)/step)
    ahead = sum(buckets.get(i,0.0) for i in range(curi+1, curi+4))
    behind= sum(buckets.get(i,0.0) for i in range(max(0,curi-3), curi))
    # Gap when behind>>ahead
    gap_score = (behind - ahead)/max(behind+ahead+1e-6,1.0)
    score = max(0.0, min(1.0, 0.5 + 0.5*gap_score))
    return score, {
        "vpvr_ahead": round(ahead,2),
        "vpvr_behind": round(behind,2),
        "gap_score": round(gap_score,2)
    }

@feature_guard
async def microcap_fresh(venue:str, symbol:str, tape:Tape) -> Tuple[float,Dict[str,Any]]:
    """'Fresh money' microcap proxy: rising trade rate + rising median size + tiny price step churn"""
    w60 = tape.window(60_000)
    if len(w60) < 30:
        return 0.0, {"reason":"tape_thin"}
    # trade rate slope (30s vs 60s)
    c30 = [x for x in w60 if int(time.time()*1000) - x[2] <= 30_000]
    tps30 = len(c30)/30.0
    tps60 = len(w60)/60.0
    tps_slope = (tps30 - tps60)/max(tps60+0.2, 0.2)
    # size shift
    sizes=[q for (_,q,_) in w60]
    sizes.sort()
    med = sizes[len(sizes)//2]
    st = STATE[(venue,symbol)]
    base = st.get("size_med_base", med)
    st["size_med_base"] = 0.98*base + 0.02*med
    size_z = (med - st["size_med_base"])/max(0.5*st["size_med_base"]+1e-6,1e-6)
    # price step churn (micro ticky behavior = accumulation)
    prices=[p for (p,_,_) in w60]
    diffs=[abs(prices[i]-prices[i-1]) for i in range(1,len(prices))]
    if not diffs:
        return 0.0, {"reason":"flat"}
    dmean = sum(diffs)/len(diffs)
    dmin  = min(diffs)
    churn = dmin/max(dmean+1e-9,1e-9)  # closer to 1 → tiny steps
    score = max(0.0, min(1.0, 0.25 + 0.25*tps_slope + 0.25*max(0.0,size_z) + 0.25*min(churn,1.0)))
    return score, {
        "tps_slope": round(tps_slope,2),
        "size_med_z": round(size_z,2),
        "tick_churn": round(min(churn,1.5),2)
    }

DETECTORS = [
    ("🚀 god_preignite(watch)", god_preignite, 0.60),
    ("📊 vpvr_gap",             vpvr_gap,      0.65),
    ("🧽 microcap_fresh",       microcap_fresh,0.55),
]

# ------------------------------ Alert & UI ------------------------------
def make_table(rows: List[Dict[str,Any]]) -> Table:
    tbl = Table(title="Explosive Signatures (last 15m)", box=box.SIMPLE_HEAVY)
    tbl.add_column("Time",         justify="left", no_wrap=True)
    tbl.add_column("Exch",         justify="left")
    tbl.add_column("Symbol",       justify="left")
    tbl.add_column("Signature",    justify="left")
    tbl.add_column("Score",        justify="right")
    tbl.add_column("Highlights",   justify="left", overflow="fold")
    for r in rows:
        sc = r["score"]
        sc_str = f"[bold green]{sc:.3f}[/bold green]" if sc>=0.75 else (f"[green]{sc:.3f}[/green]" if sc>=0.6 else f"{sc:.3f}")
        tbl.add_row(
            time.strftime("%H:%M:%S", time.localtime(r["ts"])),
            r["venue"].upper(),
            r["symbol"],
            r["name"],
            sc_str,
            r.get("highlights","")
        )
    return tbl

def push_alert(venue:str, symbol:str, name:str, score:float, details:Dict[str,Any]):
    # Store only last 15 minutes
    ALERTS.append({
        "ts": int(time.time()),
        "venue": venue,
        "symbol": symbol,
        "name": name,
        "score": score,
        "highlights": ", ".join(f"{k}={v}" for k,v in details.items() if isinstance(v,(int,float,str)) and k not in ("error","where"))[:140]
    })
    # prune >15m
    cutoff = time.time() - 900
    while ALERTS and ALERTS[0]["ts"] < cutoff:
        ALERTS.popleft()

# ------------------------------ Orchestrator ------------------------------
async def symbol_loop(q:asyncio.Queue):
    """Consume parsed events, update per-symbol tape, run detectors politely."""
    last_ui=0.0
    with Live(refresh_per_second=2, console=console, transient=False) as live:
        while True:
            # consume with timeout to keep UI responsive
            try:
                venue, symbol, kind, payload = await asyncio.wait_for(q.get(), timeout=0.5)
            except asyncio.TimeoutError:
                venue=symbol=kind=payload=None, None, None, None  # keep UI refresh
            except Exception:
                traceback.print_exc()
                continue

            if kind == "trade":
                key=(venue, symbol)
                st=STATE[key]
                tp = st.get("tape")
                if tp is None:
                    tp=Tape(maxlen=5000)
                    st["tape"]=tp
                p,q,t = payload.get("p"), payload.get("q"), int(payload.get("t") or time.time()*1000)
                if p and q:
                    tp.push(p,q,t)
                # run detectors every ~0.7s per symbol max (avoid CPU storm)
                now=time.time()
                if now - st.get("_last_run", 0) > 0.7:
                    st["_last_run"]=now
                    # Evaluate detectors
                    for (name, fn, thr) in DETECTORS:
                        score, details = await fn(venue, symbol, tp)
                        if score >= thr:
                            push_alert(venue, symbol, name, float(score), details)

            # update UI every ~0.8s
            if time.time() - last_ui > 0.8:
                last_ui=time.time()
                # sort recent by score desc
                rows = sorted(list(ALERTS), key=lambda r: (-r["score"], -r["ts"]))
                live.update(Panel(make_table(rows), border_style="cyan"))

# ------------------------------ Build venue tasks ------------------------------
def build_tasks(q:asyncio.Queue) -> List[asyncio.Task]:
    tasks=[]
    # Binance agg trades (all symbols): !aggTrade@arr
    bin_sub = {"method":"SUBSCRIBE","params":["!aggTrade@arr"],"id":1}
    bin_ws = VenueWS("binance","wss://stream.binance.com:9443/stream",bin_sub,binance_aggtrade_arr_parser)
    tasks.append(asyncio.create_task(bin_ws.run(q)))

    # Bybit unified trade stream (sample: public topic "trade.linear")
    # We subscribe per-instrument list for broad coverage; here we use "trade.all" topic (bybit auto-push)
    # If your gateway needs different, adjust parser/arg accordingly.
    async def bybit_bootstrap():
        url="wss://stream.bybit.com/v5/public/linear"
        async with aiohttp.ClientSession() as ses:
            async with ses.ws_connect(url, heartbeat=20.0) as ws:
                sub={"op":"subscribe","args":["publicTrade.BTCUSDT","publicTrade.ETHUSDT","publicTrade.*"]}
                await ws.send_json(sub)
                console.log("[WS] Bybit connected")
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        data=jloads(msg.data.encode() if isinstance(msg.data,str) else msg.data)
                        for item in bybit_trade_parser(data):
                            await q.put(item)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                        break
    tasks.append(asyncio.create_task(bybit_bootstrap()))

    # Bitget trade stream (broad)
    async def bitget_bootstrap():
        url="wss://ws.bitget.com/v2/ws/public"
        async with aiohttp.ClientSession() as ses:
            async with ses.ws_connect(url, heartbeat=20.0) as ws:
                # Subscribe to all linear USDT trade channel wildcard if supported; otherwise list common
                sub={"op":"subscribe","args":[{"instType":"SP","channel":"trade","instId":"BTCUSDT"},
                                              {"instType":"SP","channel":"trade","instId":"ETHUSDT"}]}
                await ws.send_json(sub)
                console.log("[WS] Bitget connected")
                async for msg in ws:
                    if msg.type==aiohttp.WSMsgType.TEXT:
                        data=jloads(msg.data.encode() if isinstance(msg.data,str) else msg.data)
                        if isinstance(data, dict) and data.get("event")=="subscribe": 
                            continue
                        for item in bitget_trade_parser(data):
                            await q.put(item)
                    elif msg.type in (aiohttp.WSMsgType.CLOSED,aiohttp.WSMsgType.ERROR):
                        break
    tasks.append(asyncio.create_task(bitget_bootstrap()))
    return tasks

# ------------------------------ Main ------------------------------
async def main():
    console.print("[bold cyan]Explosive Screener — Guarded Single File[/bold cyan]")
    q:asyncio.Queue = asyncio.Queue(maxsize=10000)
    tasks = build_tasks(q)
    tasks.append(asyncio.create_task(symbol_loop(q)))
    # Run forever; graceful cancel on Ctrl+C
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass
    except KeyboardInterrupt:
        console.print("[yellow]Interrupted[/yellow]")
    finally:
        for t in tasks:
            with contextlib.suppress(Exception):
                t.cancel()

if __name__=="__main__":
    try:
        asyncio.run(main())
    except RuntimeError:
        # some envs complain there is no loop; fallback
        loop=asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(main())
