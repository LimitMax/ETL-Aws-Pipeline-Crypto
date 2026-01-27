from ingestion.market.logic import fetch_data_crypto
from ingestion.market.config import CRYPTO_SYMBOLS, INTERVAL, PERIOD


def fetch_all_data() -> list[dict]:
    """
    Batch Bronze ingestion orchestrator.

    Responsibilities:
    - Iterate configured crypto symbols
    - Fetch Bronze-compatible records from yfinance
    - NO deduplication
    - NO transformation
    - Safe for backfill / replay

    Intended usage:
    - Local execution
    - Glue batch job
    """

    all_records: list[dict] = []

    for symbol in CRYPTO_SYMBOLS:
        try:
            records = fetch_data_crypto(
                symbol=symbol,
                interval=INTERVAL,
                period=PERIOD,
            )
            all_records.extend(records)

        except Exception as exc:
            # Fail-soft: continue other symbols
            print(
                f"[ERROR][batch.fetch_all_data] "
                f"symbol={symbol} error={exc}"
            )

    return all_records
