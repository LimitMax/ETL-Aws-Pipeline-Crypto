import yfinance as yf

from ingestion.market.config import SOURCE, ASSET_TYPE

def fetch_data_crypto(symbol: str, interval: str, period: str):
    """
    Fetch RAW OHLCV data from yfinance.
    Intended for bronze ingestion.
    """
    ticker = yf.Ticker(symbol)
    df = ticker.history(interval=interval, period=period)

    if df.empty:
        raise ValueError(f"No Data Returned for {symbol}")
    
    records = []

    for ts,row in df.iterrows():
        records.append({
            "datex": ts.date().isoformat(),
            "hourx": ts.hour,
            "symbol": symbol,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": float(row["Volume"]),
            "source":SOURCE,
            "asset_type": ASSET_TYPE
        })

    return records