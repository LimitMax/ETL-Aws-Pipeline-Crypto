from gold.reader import read_silver
from gold.aggregator import aggregate_daily
from gold.writer import write_gold
from gold.config import SILVER_BUCKET, GOLD_BUCKET, SILVER_PREFIX, GOLD_PREFIX

def main(dt=None):
    print("[INFO] Reading Silver...")
    df = read_silver(SILVER_BUCKET, SILVER_PREFIX, dt=dt)

    print(f"[INFO] Silver rows: {len(df)}")
    print("[INFO] Aggregating to Gold (daily)...")
    gold_df = aggregate_daily(df)

    print(f"[INFO] Gold rows: {len(gold_df)}")
    write_gold(gold_df, GOLD_BUCKET, GOLD_PREFIX)

    print("[SUCCESS] Gold layer written")

if __name__ == "__main__":
    main()