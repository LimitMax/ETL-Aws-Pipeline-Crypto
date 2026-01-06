import boto3
import pyarrow.parquet as pq
import pyarrow as pa
import os
import tempfile

s3 = boto3.client("s3")

def write_gold(df, bucket, prefix):
    if df.empty:
        print("[WARN] No Gold records to write")
        return

    for (dt, asset_type, source), part_df in df.groupby(["dt", "asset_type", "source"]):
        with tempfile.TemporaryDirectory() as tmpdir:
            local_path = os.path.join(tmpdir, "part-0000.parquet")
            table = pa.Table.from_pandas(part_df, preserve_index=False)
            pq.write_table(table, local_path, compression="snappy")

            key = (
                f"{prefix}/"
                f"asset_type={asset_type}/"
                f"source={source}/"
                f"dt={dt}/"
                f"part-0000.parquet"
            )

            s3.upload_file(local_path, bucket, key)
            print(f"[INFO] Wrote Gold Parquet: s3://{bucket}/{key}")