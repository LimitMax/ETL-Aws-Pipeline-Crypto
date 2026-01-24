from lambda_ingestion.writer import write_to_s3

def write_bronze(records, bucket: str, prefix: str):
    if not records:
        print("[WARN] No records to write to Bronze")
        return

    write_to_s3(
        records=records,
        bucket=bucket,
        prefix=prefix
    )
