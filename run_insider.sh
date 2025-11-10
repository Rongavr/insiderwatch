#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

# Your SEC contact (OK to use your email)
export SEC_EMAIL="rongavr@gmail.com"

# Activate venv
if [ -f ".venv/bin/activate" ]; then
  source .venv/bin/activate
fi

# Run the scanner and append output to insider.log
python3 insider_scanner.py --days 14 --min_owners 3 --min_usd 300000 | tee -a insider.log
