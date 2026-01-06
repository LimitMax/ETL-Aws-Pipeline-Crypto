import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
import tempfile
import os

s3 = boto3.client("s3")

def write_silver(records, bucket, prefix):
    if not records:
        print("[WARN] No records to write to Silver")
        return

    df = pd.DataFrame(records)

    required_cols = [
        "datex", "hourx", "symbol",
        "open", "high", "low", "close", "volume",
        "source", "asset_type"
    ]
    df = df[required_cols]

    for (dt, hour), part_df in df.groupby(["datex", "hourx"]):

        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "part-0000.parquet")

            table = pa.Table.from_pandas(part_df, preserve_index=False)
            pq.write_table(table, local_path, compression="snappy")

            key = (
                f"{prefix}/"
                f"asset_type=crypto/"
                f"source=yfinance/"
                f"dt={dt}/"
                f"hour={str(hour).zfill(2)}/"
                f"part-0000.parquet"
            )

            s3.upload_file(local_path, bucket, key)

            print(f"[INFO] Wrote Silver Parquet: s3://{bucket}/{key}")
