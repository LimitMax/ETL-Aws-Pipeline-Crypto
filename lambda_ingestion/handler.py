from lambda_ingestion.fetcher import fetch_raw_crypto
from lambda_ingestion.writer import write_to_s3
from lambda_ingestion.config import (
    BRONZE_BUCKET,
    BRONZE_PREFIX,
    CRYPTO_SYMBOLS
)

def lambda_handler(event, context):
    """
    AWS Lambda entry point for Bronze crypto ingestion.
    """

    if not CRYPTO_SYMBOLS:
        print("[WARN] No crypto symbols configured. Exiting.")
        return {"status": "skipped", "reason": "no symbols"}

    records = []

    for symbol in CRYPTO_SYMBOLS:
        record = fetch_raw_crypto(symbol)
        records.append(record)

    write_to_s3(
        records=records,
        bucket=BRONZE_BUCKET,
        prefix=BRONZE_PREFIX
    )

    return {
        "status": "success",
        "symbols": CRYPTO_SYMBOLS,
        "records_count": len(records)
    }
