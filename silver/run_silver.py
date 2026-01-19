from silver.reader import read_bronze_objects
from silver.writer import write_silver
from silver.config import BRONZE_BUCKET,SILVER_BUCKET,BRONZE_PREFIX,SILVER_PREFIX
from silver.transformer import bronze_to_silver
from silver.quality import validate_silver
from ingestion.market.dedup import deduplicate


def main():
    print("[INFO] Reading Bronze data...")
    bronze_records = read_bronze_objects(BRONZE_BUCKET, BRONZE_PREFIX)
    print(f"[INFO] Bronze records: {len(bronze_records)}")

    print("[INFO] Transforming Bronze → Silver (OHLCV)...")
    silver_raw = bronze_to_silver(bronze_records)
    print(f"[INFO] Silver raw records: {len(silver_raw)}")

    print("[INFO] Deduplicating Silver data...")
    silver_records = deduplicate(silver_raw)
    print(f"[INFO] Silver records after dedup: {len(silver_records)}")

    print("[INFO] Running Silver data quality checks...")
    validate_silver(silver_records)
    print("[INFO] Silver data quality checks passed")

    write_silver(
        records=silver_records,
        bucket=SILVER_BUCKET,
        prefix=SILVER_PREFIX,
    )

    print("[SUCCESS] Silver layer written")


if __name__ == "__main__":
    main()
