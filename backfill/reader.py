from ingestion.market.logic import fetch_data_crypto

def fetch_symbol_data(symbol: str, interval: str):
    return fetch_data_crypto(
        symbol=symbol,
        interval=interval,
        period="max"
    )
