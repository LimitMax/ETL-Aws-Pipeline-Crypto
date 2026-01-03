from datetime import datetime
import yfinance as yf

def fetch_data_crypto(symbol: str, interval: str, period: str):
    ticker = yf.Ticker(symbol)
    df = ticker.history(interval=interval, period=period)

    if df.empty:
        raise ValueError(f"No Data Returned for {symbol}")
    
    records = []

    for ts,row in df.iterrows():
        records.append({
            "datex": ts.date().isoformat(),   # YYYY-MM-DD
            "hourx": ts.hour,                 # 0 - 23
            "symbol": symbol,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": float(row["Volume"]),
            "source": "yfinance",
            "asset_type": "crypto"
        })

    return records