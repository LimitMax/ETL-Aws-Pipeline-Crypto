from datetime import datetime
from ingestion.market.logic import fetch_data_crypto

def bronze_to_silver(bronze_records):
    """
    Transform Bronze records into Silver OHLCV records.
    """
    silver_records = []

    for r in bronze_records:
        symbol = r["symbol"]

        # fetch OHLCV (1h, 1d)
        ohlcv_records = fetch_data_crypto(
            symbol=symbol,
            interval="1h",
            period="1d"
        )

        silver_records.extend(ohlcv_records)

    return silver_records
