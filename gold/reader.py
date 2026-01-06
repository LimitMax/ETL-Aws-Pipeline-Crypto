import boto3
import pyarrow.dataset as ds

def read_silver(bucket, prefix, dt=None):
    path = f"s3://{bucket}/{prefix}"
    if dt:
        path = f"{path}/dt={dt}/"

    dataset = ds.dataset(path, format="parquet")
    table = dataset.to_table()
    return table.to_pandas()