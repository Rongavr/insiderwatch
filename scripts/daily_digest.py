#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Build one HTML email with insider SELL trades from the last 24 hours
(US + TASE), then send it via SMTP. Robust to missing files/columns.

Inputs (CSV, if present):
  - insider_trades.csv   (US)
  - tase_trades.csv      (TASE)

Env (already in your repo secrets):
  FROM_EMAIL, MAIL_USERNAME, MAIL_PASSWORD, SMTP_SERVER, SMTP_PORT, TO_EMAIL
"""

import os
import sys
import pandas as pd
from datetime import datetime, timedelta, timezone
from email.mime.text import MIMEText
from email.header import Header
import smtplib

NOW_UTC = datetime.now(timezone.utc)
SINCE_UTC = NOW_UTC - timedelta(hours=24)

US_FILE = "insider_trades.csv"
TASE_FILE = "tase_trades.csv"

def read_csv_safe(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        return pd.DataFrame()
    try:
        df = pd.read_csv(path)
        # Trim whitespace from headers
        df.columns = [c.strip() for c in df.columns]
        return df
    except Exception as e:
        print(f"[WARN] Failed to read {path}: {e}", file=sys.stderr)
        return pd.DataFrame()

def first_nonempty(d: dict, keys, default=None):
    for k in keys:
        v = d.get(k)
        if v is not None and str(v).strip() != "":
            return v
    return default

def normalize_us(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    # Lower-case columns for flexible mapping
    cols = {c.lower(): c for c in df.columns}
    def col(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None

    out = pd.DataFrame()
    out["Market"] = "US"
    out["Company"] = df[col("company","issuer","issuer_name","name")] if col("company","issuer","issuer_name","name") else ""
    out["Ticker/Code"] = df[col("symbol","ticker")] if col("symbol","ticker") else ""
    out["Insider Name"] = df[col("insider","insider_name","holder","reporting_owner")] if col("insider","insider_name","holder","reporting_owner") else ""
    out["Role"] = df[col("relationship","role","position","title")] if col("relationship","role","position","title") else ""
    # Force action = Sell when scanner already filtered sells; else map if present
    if col("transaction","action","type"):
        out["Action"] = df[col("transaction","action","type")]
    else:
        out["Action"] = "Sell"
    out["Qty"] = df[col("shares","qty","quantity")] if col("shares","qty","quantity") else ""
    out["Avg Price"] = df[col("price","avg_price")] if col("price","avg_price") else ""
    # Value (local) is USD for US
    if col("value","est_value","value_usd","est_value_usd"):
        out["Est. Value (local)"] = df[col("value","est_value","value_usd","est_value_usd")]
    else:
        # Fallback compute if we have qty and price
        try:
            qty = pd.to_numeric(out["Qty"], errors="coerce")
            px = pd.to_numeric(out["Avg Price"], errors="coerce")
            out["Est. Value (local)"] = (qty * px).fillna("")
        except Exception:
            out["Est. Value (local)"] = ""
    out["Currency"] = "USD"

    # Dates
    td_col = col("trade_date","transaction_date","transactiondate")
    fd_col = col("filing_date","filed_date","filingdate")
    out["Trade Date"] = df[td_col] if td_col else ""
    out["Filing/Report Date"] = df[fd_col] if fd_col else out["Trade Date"]

    # Source link
    src_col = col("source_url","url","link")
    out["Source"] = df[src_col] if src_col else ""

    # Keep only sells if action present
    if "Action" in out.columns:
        out["Action"] = out["Action"].astype(str).str.title()
        out = out[out["Action"].str.contains("Sell", case=False, na=True)]

    return out

def normalize_tase(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    cols = {c.lower(): c for c in df.columns}
    def col(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None

    out = pd.DataFrame()
    out["Market"] = "TASE"
    out["Company"] = df[col("company","issuer","company_name")] if col("company","issuer","company_name") else ""
    out["Ticker/Code"] = df[col("tase_code","code","ticker")] if col("tase_code","code","ticker") else ""
    out["Insider Name"] = df[col("insider_name","insider","holder","name")] if col("insider_name","insider","holder","name") else ""
    out["Role"] = df[col("role","relationship","position","title")] if col("role","relationship","position","title") else ""
    # Often all are Sells; still map if present
    if col("action","transaction","type"):
        out["Action"] = df[col("action","transaction","type")]
    else:
        out["Action"] = "Sell"
    out["Qty"] = df[col("qty","shares","quantity")] if col("qty","shares","quantity") else ""
    # Avg price may be missing; if we have est total and qty we can compute later
    out["Avg Price"] = df[col("avg_price","price","avg_price_nis")] if col("avg_price","price","avg_price_nis") else ""
    # Est value local (NIS)
    if col("est_value_nis","est_total_nis","value_nis"):
        out["Est. Value (local)"] = df[col("est_value_nis","est_total_nis","value_nis")]
    else:
        try:
            qty = pd.to_numeric(out["Qty"], errors="coerce")
            px = pd.to_numeric(out["Avg Price"], errors="coerce")
            out["Est. Value (local)"] = (qty * px).fillna("")
        except Exception:
            out["Est. Value (local)"] = ""
    out["Currency"] = "NIS"

    td_col = col("trade_date","transaction_date","תאריך עסקה")
    rd_col = col("report_date","filing_date","when","תאריך דיווח")
    out["Trade Date"] = df[td_col] if td_col else ""
    out["Filing/Report Date"] = df[rd_col] if rd_col else out["Trade Date"]

    src_col = col("source_url","url","link")
    out["Source"] = df[src_col] if src_col else ""

    if "Action" in out.columns:
        out["Action"] = out["Action"].astype(str).str.title()
        out = out[out["Action"].str.contains("Sell", case=False, na=True)]

    return out

def parse_utc(s):
    if pd.isna(s) or s == "":
        return pd.NaT
    # Try multiple formats; assume naive = local? we'll treat as UTC for consistency
    return pd.to_datetime(s, errors="coerce", utc=True)

def filter_last_24h(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    # Prefer Filing/Report Date, fallback to Trade Date
    dates = df["Filing/Report Date"].apply(parse_utc)
    fallback = df["Trade Date"].apply(parse_utc)
    used = dates.fillna(fallback)
    mask = (used >= SINCE_UTC) & (used <= NOW_UTC)
    df = df.loc[mask].copy()
    # Sort most recent first
    df["_dt"] = used[mask]
    df = df.sort_values("_dt", ascending=False).drop(columns=["_dt"])
    return df

def fmt_int(v):
    try:
        x = float(str(v).replace(",", "").strip())
        if pd.isna(x):
            return ""
        return f"{int(x):,}"
    except Exception:
        return str(v) if v is not None else ""

def fmt_float(v):
    try:
        x = float(str(v).replace(",", "").strip())
        if pd.isna(x):
            return ""
        return f"{x:,.2f}"
    except Exception:
        return str(v) if v is not None else ""

def to_html_table(df: pd.DataFrame) -> str:
    if df.empty:
        return "<p>No insider sell trades in the last 24 hours.</p>"

    # Format numeric columns nicely
    if "Qty" in df.columns:
        df["Qty"] = df["Qty"].apply(fmt_int)
    if "Avg Price" in df.columns:
        df["Avg Price"] = df["Avg Price"].apply(fmt_float)
    if "Est. Value (local)" in df.columns:
        df["Est. Value (local)"] = df["Est. Value (local)"].apply(fmt_float)

    # Make Source a link
    if "Source" in df.columns:
        df["Source"] = df["Source"].apply(lambda u: f'<a href="{u}" target="_blank">link</a>' if isinstance(u, str) and u else "")

    cols = [
        "Market", "Company", "Ticker/Code", "Insider Name", "Role",
        "Action", "Qty", "Avg Price", "Est. Value (local)", "Currency",
        "Trade Date", "Filing/Report Date", "Source"
    ]
    df = df.reindex(columns=cols)

    style = """
      <style>
        table { border-collapse: collapse; width: 100%; font-family: Arial, sans-serif; }
        th, td { border: 1px solid #ddd; padding: 6px 8px; font-size: 13px; }
        th { background: #f5f5f5; text-align: left; }
        tr:nth-child(even) { background: #fafafa; }
      </style>
    """
    html = style + df.to_html(index=False, escape=False)
    return html

def send_email(subject: str, html_body: str):
    FROM = os.environ["FROM_EMAIL"]
    TO = os.environ["TO_EMAIL"]
    SERVER = os.environ["SMTP_SERVER"]
    PORT = int(os.environ["SMTP_PORT"])
    USER = os.environ["MAIL_USERNAME"]
    PASS = os.environ["MAIL_PASSWORD"]

    msg = MIMEText(html_body, "html", "utf-8")
    msg["Subject"] = Header(subject, "utf-8")
    msg["From"] = FROM
    msg["To"] = TO

    with smtplib.SMTP(SERVER, PORT, timeout=30) as s:
        s.starttls()
        s.login(USER, PASS)
        s.sendmail(FROM, [TO], msg.as_string())

def main():
    us = read_csv_safe(US_FILE)
    tase = read_csv_safe(TASE_FILE)

    us_n = normalize_us(us)
    tase_n = normalize_tase(tase)

    combined = pd.concat([us_n, tase_n], ignore_index=True, sort=False)
    combined = filter_last_24h(combined)

    subject = f"Insider SELLs — last 24h (US + TASE) — {NOW_UTC.strftime('%Y-%m-%d %H:%M UTC')}"
    html = to_html_table(combined)

    # Add a tiny footer for diagnostics
    html += "<br><div style='color:#777;font-size:12px;'>"
    html += f"Rows: {len(combined)} · Generated at {NOW_UTC.isoformat()} · Window since {SINCE_UTC.isoformat()}"
    html += "</div>"

    send_email(subject, html)

if __name__ == "__main__":
    main()
