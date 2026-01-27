import yfinance as yf
from datetime import datetime, timezone

from ingestion.market.config import SOURCE, ASSET_TYPE


def fetch_data_crypto(symbol: str, interval: str, period: str) -> list[dict]:
    """
    Fetch RAW OHLCV data from yfinance and emit Bronze-compatible records.

    Bronze contract (LOCKED):
    - event_time     : market event timestamp (UTC, ISO8601)
    - ingested_at    : pipeline ingestion timestamp (UTC, ISO8601)
    - NO datex/hourx
    - NO deduplication
    """

    ticker = yf.Ticker(symbol)
    df = ticker.history(interval=interval, period=period)

    if df.empty:
        raise ValueError(f"No data returned from yfinance for symbol={symbol}")

    records: list[dict] = []

    ingested_at = datetime.now(timezone.utc).isoformat()

    # yfinance index = pandas.Timestamp
    for ts, row in df.iterrows():
        # Normalize to UTC ISO format
        event_time = (
            ts.to_pydatetime()
              .replace(tzinfo=timezone.utc)
              .isoformat()
        )

        records.append({
            "symbol": symbol,
            "asset_type": ASSET_TYPE,
            "source": SOURCE,
            "event_time": event_time,
            "ingested_at": ingested_at,
            "open": float(row["Open"]),
            "high": float(row["High"]),
            "low": float(row["Low"]),
            "close": float(row["Close"]),
            "volume": float(row["Volume"]),
        })

    return records
