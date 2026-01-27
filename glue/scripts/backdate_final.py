from datetime import datetime, timedelta
import os
import json
import boto3

from ingestion.market.logic import fetch_data_crypto
from ingestion.market.config import CRYPTO_SYMBOLS, ASSET_TYPE, SOURCE

def main():
    BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET")
    BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze/crypto")
    START_DATE = os.environ.get("START_DATE")
    END_DATE = os.environ.get("END_DATE")

    if not BRONZE_BUCKET or not START_DATE or not END_DATE:
        raise RuntimeError("BRONZE_BUCKET, START_DATE, END_DATE are required")

    s3 = boto3.client("s3")

    start_dt = datetime.fromisoformat(START_DATE)
    end_dt = datetime.fromisoformat(END_DATE)

    print(f"[INFO] Bronze backdate {START_DATE} → {END_DATE}")

    current = start_dt
    while current <= end_dt:
        dt_str = current.strftime("%Y-%m-%d")
        print(f"[INFO] Processing dt={dt_str}")

        all_records = []

        for symbol in CRYPTO_SYMBOLS:
            try:
                records = fetch_data_crypto(symbol, "1h", "1d")
                daily = [r for r in records if r["datex"] == dt_str]
                all_records.extend(daily)
                print(f"[OK] {symbol} rows={len(daily)}")
            except Exception as e:
                print(f"[ERROR] {symbol}: {e}")

        if all_records:
            key = (
                f"{BRONZE_PREFIX}/"
                f"asset_type={ASSET_TYPE}/"
                f"source={SOURCE}/"
                f"dt={dt_str}/"
                f"backfill_{dt_str}.json"
            )

            s3.put_object(
                Bucket=BRONZE_BUCKET,
                Key=key,
                Body=json.dumps(all_records),
                ContentType="application/json",
            )

            print(f"[INFO] Wrote s3://{BRONZE_BUCKET}/{key}")

        current += timedelta(days=1)

    print("[SUCCESS] Bronze backdate completed")

if __name__ == "__main__":
    main()
