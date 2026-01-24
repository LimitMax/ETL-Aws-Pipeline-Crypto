import os
from datetime import datetime

BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "crypto-data-lake-limitmax")
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze/crypto")

SYMBOLS = ["BTC-USD", "ETH-USD", "BNB-USD", "SOL-USD", "ADA-USD"]

INTERVAL = "1h"
SOURCE = "yfinance"
ASSET_TYPE = "crypto"

START_DATE = os.environ.get("START_DATE", "2025-01-01")
END_DATE = datetime.utcnow().date().isoformat()

# ===== Safety Guard =====
if BRONZE_BUCKET is None:
    raise RuntimeError("BRONZE_BUCKET is not set")
