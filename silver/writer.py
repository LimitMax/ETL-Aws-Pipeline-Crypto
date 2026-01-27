import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile
from collections import defaultdict
from typing import List, Dict

s3 = boto3.client("s3")


def write_silver(
    records: List[Dict],
    bucket: str,
    prefix: str,
) -> None:
    """
    Write Silver records as partitioned Parquet.

    Partitioning:
    - asset_type
    - dt
    - hour

    Idempotency:
    - overwrite per partition (safe for 2-hour reruns)
    """

    if not records:
        print("[WARN] No Silver records to write")
        return

    groups = defaultdict(list)
    for r in records:
        key = (r["asset_type"], r["datex"], r["hourx"])
        groups[key].append(r)

    for (asset_type, dt, hour), rows in groups.items():
        table = pa.Table.from_pylist(rows)

        with tempfile.NamedTemporaryFile() as tmp:
            pq.write_table(
                table,
                tmp.name,
                compression="snappy",
                use_dictionary=True,
            )

            s3_key = (
                f"{prefix}/"
                f"asset_type={asset_type}/"
                f"dt={dt}/"
                f"hour={hour}/"
                f"part-0000.parquet"
            )

            s3.upload_file(tmp.name, bucket, s3_key)

            print(
                f"[INFO] Wrote Silver "
                f"s3://{bucket}/{s3_key} "
                f"rows={len(rows)}"
            )
