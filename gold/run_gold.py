from gold.reader import read_silver
from gold.aggregator import aggregate_daily
from gold.writer import write_gold
from gold.quality import validate_gold
from gold.config import SILVER_BUCKET,GOLD_BUCKET,SILVER_PREFIX,GOLD_PREFIX

EXPECTED_SYMBOLS = 5  # BTC, ETH, BNB, SOL, ADA


def main(dt=None):
    print("[INFO] Reading Silver...")
    df = read_silver(SILVER_BUCKET, SILVER_PREFIX, dt=dt)
    print(f"[INFO] Silver rows: {len(df)}")

    print("[INFO] Aggregating to Gold (daily)...")
    gold_df = aggregate_daily(df)
    print(f"[INFO] Gold rows: {len(gold_df)}")

    print("[INFO] Running Gold data quality checks...")
    validate_gold(gold_df, expected_symbols=EXPECTED_SYMBOLS)
    print("[INFO] Gold data quality checks passed")

    write_gold(gold_df, GOLD_BUCKET, GOLD_PREFIX)
    print("[SUCCESS] Gold layer written")


if __name__ == "__main__":
    main()
