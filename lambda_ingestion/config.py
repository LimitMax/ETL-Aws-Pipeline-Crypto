# BRONZE_BUCKET = "crypto-data-lake-limitmax"
# BRONZE_PREFIX = "bronze/crypto"

import os

# ===== Runtime Configuration =====
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET")
BRONZE_PREFIX = os.environ.get("BRONZE_PREFIX", "bronze/crypto")

# ===== Domain Configuration =====
CRYPTO_SYMBOLS = [
    "BTC-USD",
    "ETH-USD",
    "BNB-USD",
    "SOL-USD",
    "ADA-USD",
]

SOURCE = "yfinance"
ASSET_TYPE = "crypto"

# ===== Safety Guard =====
if BRONZE_BUCKET is None:
    print("[WARN] BRONZE_BUCKET is not set. S3 write will be skipped.")
