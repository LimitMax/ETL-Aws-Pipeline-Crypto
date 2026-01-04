import json
import boto3
from datetime import datetime

s3 = boto3.client("s3")

def write_silver(records, bucket, prefix):
    ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")

    key = f"{prefix}/dt={ts[:8]}/silver_crypto_{ts}.json"

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(records),
        ContentType="application/json"
    )
