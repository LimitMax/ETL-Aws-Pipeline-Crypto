import json
import boto3
from typing import Iterable, List, Dict, Optional

s3 = boto3.client("s3")


def _iter_json_keys(bucket: str, prefix: str) -> Iterable[str]:
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".json"):
                yield key


def read_bronze_objects(
    bucket: str,
    base_prefix: str,
    dt: Optional[str] = None,
    hour: Optional[str] = None,
) -> List[Dict]:
    """
    Read Bronze JSON records for Silver transformation.

    Args:
        bucket: S3 bucket name
        base_prefix: e.g. bronze/crypto
        dt: optional YYYY-MM-DD
        hour: optional HH (00-23)

    Reads ONLY the requested partitions to keep cost low.
    """

    # Build partition-aware prefix
    prefix = f"{base_prefix}/asset_type=crypto/source=yfinance"
    if dt:
        prefix = f"{prefix}/dt={dt}"
        if hour:
            prefix = f"{prefix}/hour={hour}"

    records: List[Dict] = []

    for key in _iter_json_keys(bucket, prefix):
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        data = json.loads(body)
        records.extend(data)

    return records
