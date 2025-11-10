#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# venv
if [ ! -d .venv ]; then python3 -m venv .venv; fi
source .venv/bin/activate
python3 -m pip install --upgrade pip
python3 -m pip install pandas pyarrow requests beautifulsoup4 lxml python-dateutil tqdm yfinance

# backfill in chunks (idempotent)
RANGES=(
  "2024-01-01 2024-03-31 trades_2024Q1.parquet"
  "2024-04-01 2024-06-30 trades_2024Q2.parquet"
  "2024-07-01 2024-09-30 trades_2024Q3.parquet"
  "2024-10-01 2024-12-31 trades_2024Q4.parquet"
  "2025-01-01 2025-03-31 trades_2025Q1.parquet"
  "2025-04-01 2025-06-30 trades_2025Q2.parquet"
  "2025-07-01 2025-09-30 trades_2025Q3.parquet"
  "2025-10-01 2025-11-04 trades_2025Q4toNov04.parquet"
)
for r in "${RANGES[@]}"; do
  set -- $r; s=$1; e=$2; out=$3
  if [ ! -s "$out" ]; then
    echo "Backfilling $s → $e → $out"
    python backfill_sec.py --start "$s" --end "$e" --out "$out"
  else
    echo "Skip (exists): $out"
  fi
done

# combine
python - <<'PY'
import glob,pandas as pd, os
parts=sorted(glob.glob("trades_*.parquet"))
assert parts, "No trades_*.parquet files found"
df=pd.concat([pd.read_parquet(p) for p in parts], ignore_index=True)
df.drop_duplicates(inplace=True)
df.to_parquet("trades_full.parquet")
print("Combined:", len(df), "rows from", len(parts), "parts")
PY

# build signals + evaluate
python build_signals.py --trades trades_full.parquet --out signals_full.parquet --window 14 --min_owners 3 --min_usd 300000
python evaluate.py     --signals signals_full.parquet --out report_full.csv --cost_bps 20 --horizons 5 21 63

# show outputs
ls -lh trades_full.parquet signals_full.parquet report_full.csv
echo "----- report_full.csv (top) -----"
sed -n '1,60p' report_full.csv
