import os

SILVER_BUCKET = os.environ.get("SILVER_BUCKET")
GOLD_BUCKET = os.environ.get("GOLD_BUCKET", SILVER_BUCKET)

SILVER_PREFIX = os.environ.get("SILVER_PREFIX", "silver/crypto/asset_type=crypto/source=yfinance")
GOLD_PREFIX = os.environ.get("GOLD_PREFIX", "gold/crypto")