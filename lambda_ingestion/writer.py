import json
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError

def write_to_s3(records, bucket, prefix):
    """
    Write records to S3 Bronze layer.
    - Uses IAM Role in AWS Lambda
    - Skips safely in local environment
    """

    if not bucket:
        print("[WARN] BRONZE_BUCKET not configured. Skipping S3 write.")
        return

    try:
        s3 = boto3.client("s3")

        ts = datetime.utcnow().strftime("%Y%m%d%H%M%S")
        key = f"{prefix}/dt={ts[:8]}/crypto_{ts}.json"

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
