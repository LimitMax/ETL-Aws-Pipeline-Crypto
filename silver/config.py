import os

BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET")
SILVER_BUCKET = os.environ.get("SILVER_BUCKET", BRONZE_BUCKET)

BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze/crypto/asset_type=crypto/source=yfinance")
SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "silver/crypto")
