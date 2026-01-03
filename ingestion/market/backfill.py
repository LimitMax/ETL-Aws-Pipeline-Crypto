from ingestion.market.logic import fetch_crypto_data
from ingestion.market.dedup import deduplicate

def backfill_crypto(symbol: str):
    records = fetch_crypto_data(
        symbol=symbol,
        interval="1h",
        period="max"
    )

    filtered = [
        r for r in records
        if r["datex"] >= "2024-01-01"
    ]

    return deduplicate(filtered)