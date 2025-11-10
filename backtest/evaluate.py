#!/usr/bin/env python3
import argparse, pandas as pd, numpy as np
from pandas import to_datetime
import yfinance as yf
from tqdm import tqdm

def next_trading_open(px: pd.DataFrame, t0):
    d = t0.tz_convert("UTC").date()
    days = px.index.date
    idx = np.searchsorted(days, d, side="right")
    if idx >= len(px): return None
    return px.index[idx]

def returns_at(px: pd.DataFrame, entry_idx, horizons=(5,21,63)):
    out = {}
    entry_open = px.loc[entry_idx, "Open"]
    for h in horizons:
        pos = px.index.get_loc(entry_idx)
        tgt_pos = pos + h
        if tgt_pos >= len(px):
            out[f"ret_{h}d"] = np.nan
            continue
        exit_idx = px.index[tgt_pos]
        exit_close = px.loc[exit_idx, "Adj Close"]
        out[f"ret_{h}d"] = float(exit_close/entry_open - 1.0)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--signals", default="signals.parquet")
    ap.add_argument("--out", default="report.csv")
    ap.add_argument("--cost_bps", type=float, default=20.0)
    ap.add_argument("--horizons", nargs="+", type=int, default=[5,21,63])
    args = ap.parse_args()

    sig = pd.read_parquet(args.signals)
    if sig.empty:
        print("No signals to evaluate."); return
    sig["t0"] = to_datetime(sig["t0"], utc=True)

    tickers = sorted(sig["symbol"].dropna().unique().tolist())
    data = yf.download(tickers, period="12y", interval="1d", auto_adjust=False, group_by="ticker", progress=False)

    rows = []
    for _, s in tqdm(sig.iterrows(), total=len(sig)):
        sym, t0 = s["symbol"], s["t0"]
        px = data if len(tickers)==1 else data[sym]
        px = px.dropna()
        if px.empty: continue
        entry_idx = next_trading_open(px, t0)
        if entry_idx is None: continue
        res = {"symbol": sym, "t0": t0, "entry_idx": entry_idx, "owners_count": s["owners_count"], "total_usd": s["total_usd"]}
        res.update(returns_at(px, entry_idx, tuple(args.horizons)))
        rows.append(res)

    ev = pd.DataFrame(rows)
    if ev.empty:
        print("No evaluable signals."); return

    cost = args.cost_bps/10000.0
    for h in args.horizons:
        ev[f"ret_{h}d_net"] = ev[f"ret_{h}d"] - cost

    print("\n=== Summary (net) ===")
    for h in args.horizons:
        col = f"ret_{h}d_net"
        valid = ev[col].dropna()
        if valid.empty:
            print(f"{h}d: no data"); continue
        print(f"{h}d: avg {valid.mean():.3%} | median {valid.median():.3%} | hit% {(valid>0).mean():.1%} | n={len(valid)}")

    ev.to_csv(args.out, index=False)
    print(f"\nSaved per-signal results -> {args.out}")

if __name__ == "__main__":
    main()
