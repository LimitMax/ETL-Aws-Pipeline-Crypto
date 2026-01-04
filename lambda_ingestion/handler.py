from lambda_ingestion.fetcher import fetch_raw_crypto
from lambda_ingestion.writer import write_to_s3
from lambda_ingestion.config import (
    BRONZE_BUCKET,
    BRONZE_PREFIX,
    CRYPTO_SYMBOLS,
    ASSET_TYPE,
    SOURCE
)

def lambda_handler(event, context):
    records = []

    for symbol in CRYPTO_SYMBOLS:
        records.append(fetch_raw_crypto(symbol))

    write_to_s3(
        records=records,
        bucket=BRONZE_BUCKET,
        prefix=BRONZE_PREFIX,
        asset_type=ASSET_TYPE,
        source=SOURCE
    )

    return {
        "status": "success",
        "records_count": len(records)
    }
