#!/usr/bin/env python3
# Temporary TASE stub: creates empty CSVs and (optionally) emails to confirm wiring.
import os, ssl, smtplib
from email.mime.text import MIMEText
from datetime import datetime, timezone
import pandas as pd

MAIL_USERNAME = os.getenv("MAIL_USERNAME")
MAIL_PASSWORD = os.getenv("MAIL_PASSWORD")
FROM_EMAIL    = os.getenv("FROM_EMAIL", MAIL_USERNAME)
TO_EMAIL      = os.getenv("TO_EMAIL", MAIL_USERNAME)

# Create empty TASE CSV artifacts (schemas we’ll populate later)
pd.DataFrame(columns=[
    "symbol","owner","shares","price_nis","amount_nis","txn_date","report_url"
]).to_csv("insider_trades_tase.csv", index=False)

pd.DataFrame(columns=[
    "symbol","owners_count","total_nis","last_when"
]).to_csv("alerts_tase.csv", index=False)

subject = "[InsiderWatch] TASE Insider SELLs — stub OK"
body = (
    "TASE support stub ran at "
    f"{datetime.now(timezone.utc).isoformat()} UTC.\n"
    "This only confirms wiring. Next step will connect to ISA/MAYA.\n"
    "Artifacts created: insider_trades_tase.csv, alerts_tase.csv (empty)."
)

def maybe_email():
    if not (MAIL_USERNAME and MAIL_PASSWORD and FROM_EMAIL and TO_EMAIL):
        print("[TASE] Missing mail envs; skipped email. (Stub succeeded.)")
        return
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = FROM_EMAIL
    msg["To"] = TO_EMAIL
    with smtplib.SMTP("smtp.gmail.com", 587) as s:
        s.starttls(context=ssl.create_default_context())
        s.login(MAIL_USERNAME, MAIL_PASSWORD)
        s.sendmail(FROM_EMAIL, [TO_EMAIL], msg.as_string())

print(body)
maybe_email()
