import boto3
import json

s3 = boto3.client("s3")

def read_bronze_objects(bucket, prefix):
    paginator = s3.get_paginator("list_objects_v2")
    records = []

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if not key.endswith(".json"):
                print(f"[DEBUG] Skipping non-json object: {key}")
                continue

            response = s3.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()

            if not body:
                print(f"[WARN] Skipping empty file: {key}")
                continue

            try:
                data = json.loads(body)

                if isinstance(data, list):
                    records.extend(data)
                elif isinstance(data, dict):
                    records.append(data)
                else:
                    print(f"[WARN] Unknown JSON structure in {key}")

            except json.JSONDecodeError:
                print(f"[ERROR] Invalid JSON in {key}, skipping")

    return records
