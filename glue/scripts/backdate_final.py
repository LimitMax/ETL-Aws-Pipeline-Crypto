import argparse
import json
import boto3
import yfinance as yf
from datetime import datetime, timedelta
import pandas as pd

# ======================
# Argument Parser
# ======================
parser = argparse.ArgumentParser()
parser.add_argument("--BRONZE_BUCKET", required=True)
parser.add_argument("--BRONZE_PREFIX", default="bronze/crypto")
parser.add_argument("--START_DATE", required=True)
parser.add_argument("--END_DATE", default=None)

args, _ = parser.parse_known_args()

BRONZE_BUCKET = args.BRONZE_BUCKET
BRONZE_PREFIX = args.BRONZE_PREFIX
START_DATE = args.START_DATE
END_DATE = args.END_DATE or datetime.utcnow().strftime("%Y-%m-%d")

# ======================
# Config
# ======================
SYMBOLS = [
    "BTC-USD",
    "ETH-USD",
    "BNB-USD",
    "SOL-USD",
    "ADA-USD",
]

ASSET_TYPE = "crypto"
SOURCE = "yfinance"

s3 = boto3.client("s3")

print(f"[INFO] Backfill range: {START_DATE} → {END_DATE}")
print(f"[INFO] Bronze bucket: {BRONZE_BUCKET}")
print(f"[INFO] Bronze prefix: {BRONZE_PREFIX}")

# ======================
# Main Logic
# ======================
start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")

for symbol in SYMBOLS:
    print(f"\n[INFO] Processing symbol: {symbol}")

    try:
        df = yf.download(
            symbol,
            start=START_DATE,
            end=(end_dt + timedelta(days=1)).strftime("%Y-%m-%d"),
            interval="1h",
            progress=False
        )

        if df.empty:
            print(f"[WARN] No data for {symbol}")
            continue

        df.reset_index(inplace=True)
        df["dt"] = df["Datetime"].dt.strftime("%Y-%m-%d")

        for dt, group in df.groupby("dt"):
            records = []
            for _, row in group.iterrows():
                records.append({
                "symbol": symbol,
                "asset_type": ASSET_TYPE,
                "source": SOURCE,
                "datetime": str(row["Datetime"]),
                "open": float(row["Open"]),
                "high": float(row["High"]),
                "low": float(row["Low"]),
                "close": float(row["Close"]),
                "volume": float(row["Volume"]),
            })

            s3_key = (
                f"{BRONZE_PREFIX}/"
                f"asset_type={ASSET_TYPE}/"
                f"source={SOURCE}/"
                f"dt={dt}/"
                f"{symbol.replace('-', '_')}_{dt}.json"
            )

            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=s3_key,
                Body=json.dumps(records),
                ContentType="application/json"
            )

            print(f"[SUCCESS] Wrote {symbol} dt={dt} ({len(records)} rows)")

    except Exception as e:
        print(f"[ERROR] Failed symbol {symbol}: {e}")

print("\n[DONE] Bronze backfill completed")
