import json
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError

def write_to_s3(records, bucket, prefix, asset_type, source):
    """
    Write raw records to S3 Bronze with partitioned layout.
    """

    if not bucket:
        print("[WARN] BRONZE_BUCKET not configured. Skipping S3 write.")
        return

    try:
        s3 = boto3.client("s3")

        now = datetime.utcnow()
        dt = now.strftime("%Y-%m-%d")
        hour = now.strftime("%H")
        ts = now.strftime("%Y%m%d%H%M%S")

        key = (
            f"{prefix}/"
            f"asset_type={asset_type}/"
            f"source={source}/"
            f"dt={dt}/"
            f"hour={hour}/"
            f"crypto_{ts}.json"
        )

        s3.put_object(
            Bucket=bucket,
            Key=key,
            Body=json.dumps(records),
            ContentType="application/json"
        )

        print(f"[INFO] Wrote {len(records)} records to s3://{bucket}/{key}")

    except NoCredentialsError:
        print("[WARN] AWS credentials not found. Skipping S3 write (local run).")

    except Exception as e:
        print(f"[ERROR] Failed to write to S3: {str(e)}")
        raise
