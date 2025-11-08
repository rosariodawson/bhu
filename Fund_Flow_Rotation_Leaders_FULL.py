#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Fund Flow Rotation Leaders - FULL
CoinPaprika (no key) tickers + tags
- Inflow Tables (Top-10 Net Inflows 24h; Top-10 Continuation proxy 24-48h)
- Rotation Predictor (groups)
- Category Liquidity Leaders for predicted group (e.g., 'Memes')
- Rich TUI; gentle rate-limit backoff
"""
import argparse, time, sys, math, re
from typing import Dict, List, Tuple
import requests
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.text import Text

API = "https://api.coinpaprika.com/v1"

def _get(path: str, params: dict | None = None, retries: int = 6):
    url = API + path
    backoff = 0.7
    for _ in range(retries):
        try:
            r = requests.get(url, params=params, timeout=15)
            if r.status_code == 200:
                return r.json()
            time.sleep(backoff)
            backoff *= 1.6
        except Exception:
            time.sleep(backoff)
            backoff *= 1.6
    return None

def fetch_all_tickers(quotes="USD") -> pd.DataFrame:
    data = _get("/tickers", params={"quotes": quotes})
    if not isinstance(data, list):
        return pd.DataFrame()
    rows = []
    for c in data:
        q = (c.get("quotes") or {}).get(quotes, {})
        rows.append({
            "id": c.get("id"),
            "name": c.get("name"),
            "symbol": c.get("symbol"),
            "mcap": q.get("market_cap"),
            "vol24h": q.get("volume_24h"),
            "pct_1h": q.get("percent_change_1h"),
            "pct_24h": q.get("percent_change_24h"),
            "pct_7d": q.get("percent_change_7d"),
        })
    return pd.DataFrame(rows).dropna(subset=["id", "mcap"]).reset_index(drop=True)

def fetch_tags_with_coins() -> pd.DataFrame:
    data = _get("/tags", params={"additional_fields": "coins"})
    if not isinstance(data, list):
        return pd.DataFrame()
    rows = []
    for t in data:
        rows.append({
            "tag_id": t.get("id"),
            "name": t.get("name"),
            "type": t.get("type"),
            "description": t.get("description"),
            "coin_ids": t.get("coins") or [],
            "coin_count": len(t.get("coins") or []),
        })
    return pd.DataFrame(rows)

GROUP_MAP = {
    "Memes": [r"\bmeme\b", r"doge", r"pepe"],
    "AI": [r"\bai\b", r"artificial intelligence", r"agent"],
    "Gaming": [r"gaming", r"\bgame\b", r"metaverse", r"play[- ]?to[- ]?earn"],
    "DeFi": [r"defi", r"amm", r"dex", r"lending", r"yield"],
    "L1": [r"layer-?1", r"smart contract", r"platform"],
    "L2": [r"layer-?2", r"rollup", r"zk"],
    "RWA": [r"\brwa\b", r"tokenized real world"],
    "Infra": [r"infrastructure", r"oracle", r"index", r"data"],
}

def _assign_group(tag_name: str) -> str:
    if not isinstance(tag_name, str):
        return "Other"
    s = tag_name.lower()
    for g, pats in GROUP_MAP.items():
        for p in pats:
            if re.search(p, s):
                return g
    return "Other"

def _z(series: pd.Series) -> pd.Series:
    s = pd.to_numeric(series, errors="coerce").astype(float)
    mu = s.mean()
    sd = s.std(ddof=0)
    if not (sd and sd == sd and sd != 0):
        return pd.Series([0.0]*len(s), index=s.index)
    return (s - mu) / sd

def _fmt_usd(x: float) -> str:
    try:
        x = float(x)
    except Exception:
        return "-"
    neg = x < 0
    ax = abs(x)
    if ax >= 1e12: s = f"{ax/1e12:.2f}T"
    elif ax >= 1e9: s = f"{ax/1e9:.2f}B"
    elif ax >= 1e6: s = f"{ax/1e6:.2f}M"
    elif ax >= 1e3: s = f"{ax/1e3:.0f}K"
    else: s = f"{ax:.0f}"
    return f"-{s}" if neg else s

def build_inflow_frame(coins_df: pd.DataFrame) -> pd.DataFrame:
    df = coins_df.copy()
    for col in ["mcap","pct_1h","pct_24h","pct_7d","vol24h"]:
        df[col] = pd.to_numeric(df.get(col), errors="coerce")
    df["inflow_1h_usd"] = (df["mcap"] * (df["pct_1h"] / 100.0)).fillna(0.0)
    df["inflow_24h_usd"] = (df["mcap"] * (df["pct_24h"] / 100.0)).fillna(0.0)
    z_in1 = _z(df["inflow_1h_usd"]); z_p1 = _z(df["pct_1h"]); z_p24 = _z(df["pct_24h"]); z_p7 = _z(df["pct_7d"]); z_v24 = _z(df["vol24h"])
    df["continuation_score"] = (1.0*z_in1 + 0.5*z_p1 + 0.4*z_p24 + 0.2*z_p7 + 0.3*z_v24)
    return df

def _money_cell(x: float) -> str:
    v = float(x or 0)
    s = _fmt_usd(v)
    if v > 0: return f"[green]{s}[/]"
    if v < 0: return f"[red]{s}[/]"
    return f"[dim]{s}[/]"

def _pct_cell(p: float) -> str:
    try:
        v = float(p or 0.0)
    except Exception:
        v = 0.0
    color = "green" if v > 0 else ("red" if v < 0 else "dim")
    return f"[{color}]{v:+.2f}%[/]"

def _score_cell(v: float) -> str:
    v = float(v or 0.0)
    if v >= 3: return f"[bold bright_green]{v:.2f}[/]"
    if v >= 2: return f"[green]{v:.2f}[/]"
    if v >= 1: return f"[yellow]{v:.2f}[/]"
    return f"[dim]{v:.2f}[/]"

def print_inflow_tables(console: Console, coins_df: pd.DataFrame, top_n: int = 10):
    df = build_inflow_frame(coins_df)
    t1 = Table(title="Top 10 - Net Inflows (24h, market-cap delta proxy)", expand=True)
    for col, just in [("#","right"),("Symbol","left"),("Name","left"),("Inflow 24h","right"),("Inflow 1h","right"),("MCap","right"),("Vol24h","right"),("%1h","right"),("%24h","right"),("%7d","right")]:
        t1.add_column(col, justify=just, no_wrap=True)
    top_in = df.sort_values("inflow_24h_usd", ascending=False).head(top_n)
    for i, r in enumerate(top_in.itertuples(index=False), 1):
        t1.add_row(
            str(i), getattr(r,"symbol",""), getattr(r,"name",""),
            _money_cell(getattr(r,"inflow_24h_usd",0)),
            _money_cell(getattr(r,"inflow_1h_usd",0)),
            _fmt_usd(getattr(r,"mcap",0)),
            _fmt_usd(getattr(r,"vol24h",0)),
            _pct_cell(getattr(r,"pct_1h",0)),
            _pct_cell(getattr(r,"pct_24h",0)),
            _pct_cell(getattr(r,"pct_7d",0)),
        )
    console.print(t1)

    t2 = Table(title="Top 10 - Likely Continuation (24-48h proxy)", expand=True)
    for col, just in [("#","right"),("Symbol","left"),("Name","left"),("Score","right"),("Inflow 1h","right"),("Inflow 24h","right"),("MCap","right"),("Vol24h","right"),("%1h","right"),("%24h","right"),("%7d","right")]:
        t2.add_column(col, justify=just, no_wrap=True)
    top_ct = df.sort_values("continuation_score", ascending=False).head(top_n)
    for i, r in enumerate(top_ct.itertuples(index=False), 1):
        t2.add_row(
            str(i), getattr(r,"symbol",""), getattr(r,"name",""),
            _score_cell(getattr(r,"continuation_score",0)),
            _money_cell(getattr(r,"inflow_1h_usd",0)),
            _money_cell(getattr(r,"inflow_24h_usd",0)),
            _fmt_usd(getattr(r,"mcap",0)),
            _fmt_usd(getattr(r,"vol24h",0)),
            _pct_cell(getattr(r,"pct_1h",0)),
            _pct_cell(getattr(r,"pct_24h",0)),
            _pct_cell(getattr(r,"pct_7d",0)),
        )
    console.print(t2)

def build_group_rotation(tick: pd.DataFrame, tags: pd.DataFrame) -> pd.DataFrame:
    if tick.empty or tags.empty: return pd.DataFrame()
    tags = tags.copy()
    tags["group"] = tags["name"].map(_assign_group)
    out_rows = []
    tix = tick.set_index("id")
    for _, row in tags.iterrows():
        ids = [cid for cid in (row["coin_ids"] or []) if cid in tix.index]
        if len(ids) < 5: 
            continue
        sub = tix.loc[ids]
        total_mcap = pd.to_numeric(sub["mcap"], errors="coerce").sum()
        vol24 = pd.to_numeric(sub["vol24h"], errors="coerce").sum()
        mean_1h = pd.to_numeric(sub["pct_1h"], errors="coerce").mean()
        mean_24 = pd.to_numeric(sub["pct_24h"], errors="coerce").mean()
        breadth_1h = (pd.to_numeric(sub["pct_1h"], errors="coerce") > 0).mean()
        turnover = (vol24 / total_mcap) if total_mcap > 0 else 0.0
        rot_score = 0.45*mean_1h + 0.25*mean_24 + 0.20*(breadth_1h*100) + 0.10*(turnover*100)
        out_rows.append({
            "group": row["group"],
            "tag_name": row["name"],
            "coins": len(ids),
            "mcap": total_mcap,
            "vol24h": vol24,
            "mean_1h": mean_1h,
            "mean_24h": mean_24,
            "breadth_1h%": breadth_1h*100,
            "turnover%": turnover*100,
            "rot_score": rot_score,
        })
    df = pd.DataFrame(out_rows)
    if df.empty: return df
    g = df.groupby("group", as_index=False).agg({
        "mcap":"sum","vol24h":"sum",
        "mean_1h":"mean","mean_24h":"mean",
        "breadth_1h%":"mean","turnover%":"mean",
        "rot_score":"mean"
    })
    return g.sort_values("rot_score", ascending=False).reset_index(drop=True)

def print_rotation_table(console: Console, g: pd.DataFrame, top_groups: int = 8):
    if g.empty:
        console.print("No rotation signals.", style="bold red")
        return
    t = Table(title="Rotation Predictor - Groups (next 24h)", expand=True)
    t.add_column("#", justify="right"); t.add_column("Group", style="bold")
    t.add_column("RotScore", justify="right"); t.add_column("MCap", justify="right")
    t.add_column("Vol24h", justify="right"); t.add_column("Thrust 1h", justify="right")
    t.add_column("Follow 24h", justify="right"); t.add_column("Breadth 1h", justify="right")
    t.add_column("Turnover", justify="right")
    view = g.head(top_groups)
    for i, r in enumerate(view.itertuples(index=False), 1):
        t.add_row(
            str(i), r.group, _score_cell(r.rot_score/10.0),
            _fmt_usd(r.mcap), _fmt_usd(r.vol24h),
            _pct_cell(r.mean_1h), _pct_cell(r.mean_24h),
            _pct_cell(r._asdict()["breadth_1h%"]), _pct_cell(r._asdict()["turnover%"]),
        )
    console.print(t)

STABLES = {"USDT","USDC","DAI","TUSD","FDUSD","USDE","USDD","PYUSD","GUSD","EURT","BUSD"}

def _category_coin_ids(tags_df: pd.DataFrame, group_name: str):
    t = tags_df.copy()
    t["group"] = t["name"].map(_assign_group)
    ids = []
    for _, r in t[t["group"] == group_name].iterrows():
        ids.extend(r.get("coin_ids") or [])
    return list(dict.fromkeys(ids))

def fetch_coin_markets(coin_id: str, quotes="USD", retries=4):
    backoff = 0.8
    for _ in range(retries):
        data = _get(f"/coins/{coin_id}/markets", params={"quotes": quotes})
        if isinstance(data, list):
            return data
        time.sleep(backoff); backoff *= 1.6
    return []

def _venue_metrics(markets_list):
    vols = []
    venues = {}
    for m in markets_list:
        q = (m.get("quotes") or {}).get("USD", {})
        v = float(q.get("volume_24h") or 0.0)
        ex = m.get("exchange_id") or "?"
        vols.append(v); venues[ex] = venues.get(ex, 0.0) + v
    total = sum(vols) or 0.0
    venue_cnt = len(venues)
    top_share = (max(venues.values())/total) if total>0 and venues else 0.0
    return total, venue_cnt, top_share

def compute_category_leaders(group_name: str, tick_df: pd.DataFrame, tags_df: pd.DataFrame, deep=False, max_deep=60):
    ids = _category_coin_ids(tags_df, group_name)
    if not ids: return pd.DataFrame()
    tix = tick_df.set_index("id")
    coins = tix.loc[tix.index.intersection(ids)].reset_index()
    if coins.empty: return coins
    coins = coins[~coins["symbol"].isin(STABLES)].copy()
    coins["mcap"] = pd.to_numeric(coins["mcap"], errors="coerce")
    coins["vol24h"] = pd.to_numeric(coins["vol24h"], errors="coerce")
    coins = coins[(coins["mcap"] > 5_000_000) & (coins["vol24h"] > 1_000_000)].copy()
    alpha = 0.6
    coins["turnover"] = (coins["vol24h"]/coins["mcap"]).clip(lower=0)
    coins["size_neutral_vol"] = coins["vol24h"]/(coins["mcap"].pow(alpha).clip(lower=1))
    z_turn = _z(coins["turnover"]); z_size = _z(coins["size_neutral_vol"]); z_abs = _z(coins["vol24h"])
    coins["LiquidityScore"] = 0.45*z_turn + 0.35*z_size + 0.20*z_abs
    if deep:
        rows = []
        for _, r in coins.head(max_deep).iterrows():
            mkts = fetch_coin_markets(r["id"])
            tot, venue_cnt, top_share = _venue_metrics(mkts)
            rows.append((r["id"], tot, venue_cnt, top_share)); time.sleep(0.2)
        venue_df = pd.DataFrame(rows, columns=["id","venue_usd","venue_cnt","top_share"])
        coins = coins.merge(venue_df, on="id", how="left")
        coins["venue_cnt"] = coins["venue_cnt"].fillna(0)
        coins["top_share"] = coins["top_share"].fillna(1.0)
        coins["LiquidityScore"] += 0.15*_z(coins["venue_cnt"]) + 0.15*_z(-coins["top_share"])
    coins["Turnover%"] = (coins["turnover"]*100).map(lambda x:f"{x:.2f}%")
    coins["Vol24h$"] = coins["vol24h"].map(_fmt_usd)
    coins["MCap$"] = coins["mcap"].map(_fmt_usd)
    if deep:
        coins["Venues"] = coins["venue_cnt"].fillna(0).astype(int)
        coins["TopVenue%"] = (coins["top_share"]*100).map(lambda x:f"{x:.1f}%")
    return coins.sort_values("LiquidityScore", ascending=False).reset_index(drop=True)

def print_category_leaders(console: Console, group_name: str, coins: pd.DataFrame, top_n=10, deep=False):
    title = f"Category Liquidity Leaders - {group_name} (next 24h)"
    t = Table(title=title, expand=True)
    t.add_column("#", justify="right"); t.add_column("Symbol", style="bold"); t.add_column("Name")
    t.add_column("LiquidityScore", justify="right"); t.add_column("Vol24h", justify="right")
    t.add_column("MCap", justify="right"); t.add_column("Turnover", justify="right")
    if deep: t.add_column("Venues", justify="right"); t.add_column("TopVenue%", justify="right")
    view = coins.head(top_n)
    for i, r in enumerate(view.itertuples(index=False), 1):
        score = float(getattr(r,"LiquidityScore",0.0))
        if score >= 3: score_str = f"[bold bright_green]{score:.2f}[/]"
        elif score >= 2: score_str = f"[green]{score:.2f}[/]"
        elif score >= 1: score_str = f"[yellow]{score:.2f}[/]"
        else: score_str = f"[dim]{score:.2f}[/]"
        row = [str(i), getattr(r,"symbol",""), getattr(r,"name",""),
               score_str, getattr(r,"Vol24h$",""), getattr(r,"MCap$",""),
               getattr(r,"Turnover%","")]
        if deep:
            row += [str(getattr(r,"Venues",0)), getattr(r,"TopVenue%","")]
        t.add_row(*row)
    console.print(t)

def main():
    ap = argparse.ArgumentParser(description="Fund Flow Rotation Leaders (CoinPaprika, no key)")
    ap.add_argument("--top", type=int, default=10, help="Top-N rows for inflow tables")
    ap.add_argument("--groups", type=int, default=8, help="Top-N groups for rotation predictor")
    ap.add_argument("--leaders_for", type=str, default="", help="Category to show leaders for (default: auto-pick top predicted group)")
    ap.add_argument("--leaders_top", type=int, default=10, help="How many leaders to show")
    ap.add_argument("--deep_liquidity", action="store_true", help="Use /coins/{id}/markets for venue metrics (rate-limited)")
    ap.add_argument("--max_deep", type=int, default=60, help="Max coins to inspect in deep mode")
    ap.add_argument("--no_color", action="store_true")
    args = ap.parse_args()

    console = Console(no_color=args.no_color)
    console.print("Fetching tickers & tags…", style="dim")
    tick = fetch_all_tickers("USD")
    tags = fetch_tags_with_coins()
    if tick.empty or tags.empty:
        console.print("No data (network or rate-limit). Try again.", style="bold red")
        sys.exit(1)

    print_inflow_tables(console, tick, top_n=args.top)

    g = build_group_rotation(tick, tags)
    print_rotation_table(console, g, top_groups=args.groups)

    group_for = args.leaders_for.strip() or (g["group"].iloc[0] if not g.empty else "Memes")
    leaders = compute_category_leaders(group_for, tick, tags, deep=args.deep_liquidity, max_deep=args.max_deep)
    if leaders.empty:
        console.print(f"No coins found for category '{group_for}'.", style="yellow")
    else:
        print_category_leaders(console, group_for, leaders, top_n=args.leaders_top, deep=args.deep_liquidity)

if __name__ == "__main__":
    main()
