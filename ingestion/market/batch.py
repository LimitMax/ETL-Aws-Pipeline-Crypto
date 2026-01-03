from ingestion.market.logic import fetch_data_crypto
from ingestion.market.dedup import deduplicate
from ingestion.market.config import CRYPTO_SYMBOLS,INTERVAL,PERIOD


def fetch_all_data():
    all_data = []

    for symbol in CRYPTO_SYMBOLS:
        try:
            records = fetch_data_crypto(
                symbol=symbol,
                interval=INTERVAL,
                period=PERIOD
            )
            all_data.extend(records)
        except Exception as e:
            print(f"[ERROR][fetch_all_data] symbol={symbol} error={str(e)}")

    return  deduplicate(all_data) 