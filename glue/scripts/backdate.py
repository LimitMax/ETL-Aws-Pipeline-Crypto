import os
import json
import boto3
from datetime import datetime, timedelta, date

from ingestion.market.logic import fetch_crypto_data
from ingestion.market.dedup import deduplicate
from ingestion.market.config import CRYPTO_SYMBOLS, SOURCE, ASSET_TYPE

# =========================
# Runtime Configuration
# =========================
BRONZE_BUCKET = os.environ["BRONZE_BUCKET"]
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze/crypto")

START_DATE_STR = os.environ.get("START_DATE")
if not START_DATE_STR:
    raise ValueError("START_DATE must be provided (format: YYYY-MM-DD)")

START_DATE = datetime.strptime(START_DATE_STR, "%Y-%m-%d").date()

END_DATE = date.today()

s3 = boto3.client("s3")


# =========================
# Helpers
# =========================
def date_range(start: date, end: date):
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def write_bronze_records(records: list, symbol: str, dt: date):
    if not records:
        return

    key = (
        f"{BRONZE_PREFIX}/"
        f"asset_type={ASSET_TYPE}/"
        f"source={SOURCE}/"
        f"symbol={symbol}/"
        f"dt={dt.isoformat()}/"
        f"data.json"
    )

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=key,
        Body=json.dumps(records),
        ContentType="application/json",
    )

    print(f"[INFO] Wrote {len(records)} records → s3://{BRONZE_BUCKET}/{key}")


# =========================
# Main Backfill Flow
# =========================
def main():
    print("[INFO] Starting Bronze backfill")
    print(f"[INFO] Date range: {START_DATE} → {END_DATE}")
    print(f"[INFO] Symbols: {CRYPTO_SYMBOLS}")

    for symbol in CRYPTO_SYMBOLS:
        print(f"[INFO] Processing symbol={symbol}")

        for dt in date_range(START_DATE, END_DATE):
            try:
                records = fetch_crypto_data(
                    symbol=symbol,
                    interval="1h",
                    start_date=dt,
                    end_date=dt + timedelta(days=1),
                )

                records = deduplicate(records)
                write_bronze_records(records, symbol, dt)

            except Exception as e:
                print(
                    f"[ERROR] symbol={symbol} date={dt} error={str(e)}"
                )

    print("[SUCCESS] Bronze backfill completed")


if __name__ == "__main__":
    main()
