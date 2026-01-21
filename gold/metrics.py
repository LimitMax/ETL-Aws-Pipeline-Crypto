import os
import boto3

def emit_gold_row_count(dt: str, row_count: int):
    # Skip metrics in local execution
    if os.getenv("ENV", "local") == "local":
        print(f"[METRIC][SKIPPED][LOCAL] GoldRowCount dt={dt} value={row_count}")
        return

    cloudwatch = boto3.client("cloudwatch")
    cloudwatch.put_metric_data(
        Namespace="CryptoPipeline",
        MetricData=[
            {
                "MetricName": "GoldRowCount",
                "Dimensions": [
                    {"Name": "dt", "Value": dt}
                ],
                "Value": row_count,
                "Unit": "Count"
            }
        ]
    )
