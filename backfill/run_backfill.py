from backfill.config import (
    BRONZE_BUCKET,
    BRONZE_PREFIX,
    CRYPTO_SYMBOLS,
    INTERVAL,
    START_DATE,
    END_DATE,
)

from backfill.reader import fetch_symbol_data
from backfill.transformer import filter_date_range
from backfill.writer import write_bronze
from ingestion.market.dedup import deduplicate


def main():
    print("[INFO] Starting Bronze Backfill Job")
    print(f"[INFO] Backfill range: {START_DATE} → {END_DATE}")

    all_records = []

    for symbol in CRYPTO_SYMBOLS:
        print(f"[INFO] Fetching symbol={symbol}")

        raw_records = fetch_symbol_data(
            symbol=symbol,
            interval=INTERVAL
        )

        filtered = filter_date_range(
            records=raw_records,
            start_date=START_DATE,
            end_date=END_DATE
        )

        all_records.extend(filtered)

    print(f"[INFO] Raw records collected: {len(all_records)}")

    print("[INFO] Deduplicating records...")
    final_records = deduplicate(all_records)
    print(f"[INFO] Records after deduplication: {len(final_records)}")

    write_bronze(
        records=final_records,
        bucket=BRONZE_BUCKET,
        prefix=BRONZE_PREFIX
    )

    print("[SUCCESS] Bronze backfill completed")


if __name__ == "__main__":
    main()
