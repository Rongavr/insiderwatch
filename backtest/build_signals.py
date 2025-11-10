#!/usr/bin/env python3
import argparse, pandas as pd
from pandas import to_datetime

def first_cross_events(df, window_days=14, min_owners=3, min_usd=300000, exclude_10b5=True):
    df = df.copy()
    fd = to_datetime(df.get("filing_dt"), errors="coerce", utc=True)
    df["_t"] = fd
    if exclude_10b5 and "tenb5" in df.columns:
        df = df[~df["tenb5"].astype(bool)]
    df = df.dropna(subset=["symbol","_t","amount_usd","owner"])
    df["amount_usd"] = pd.to_numeric(df["amount_usd"], errors="coerce")
    df = df.sort_values(["symbol","_t"])
    signals = []
    for sym, g in df.groupby("symbol", sort=False):
        g = g.reset_index(drop=True)
        for _, row in g.iterrows():
            t0 = row["_t"]
            w  = g[(g["_t"]>=t0 - pd.Timedelta(days=window_days)) & (g["_t"]<=t0)]
            owners = w["owner"].nunique()
            tots   = w["amount_usd"].sum()
            w_prev = g[(g["_t"]>=t0 - pd.Timedelta(days=window_days)) & (g["_t"]<t0)]
            crossed_prev = (w_prev["owner"].nunique()>=min_owners) and (w_prev["amount_usd"].sum()>=min_usd)
            crossed_now  = (owners>=min_owners) and (tots>=min_usd)
            if crossed_now and not crossed_prev:
                signals.append({"symbol": sym, "t0": t0, "owners_count": int(owners), "total_usd": float(tots)})
    return pd.DataFrame(signals)

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trades", default="trades.parquet")
    ap.add_argument("--out",    default="signals.parquet")
    ap.add_argument("--window", type=int, default=14)
    ap.add_argument("--min_owners", type=int, default=3)
    ap.add_argument("--min_usd", type=float, default=300000)
    ap.add_argument("--exclude_10b5", type=int, default=1)
    args = ap.parse_args()

    df = pd.read_parquet(args.trades)
    sig = first_cross_events(
        df,
        window_days=args.window,
        min_owners=args.min_owners,
        min_usd=args.min_usd,
        exclude_10b5=bool(args.exclude_10b5),
    )
    if sig.empty:
        print("No signals.")
    else:
        sig.to_parquet(args.out, index=False)
        print(f"Signals: {len(sig):,} -> {args.out}")
        print(sig.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
