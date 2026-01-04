from datetime import datetime
from lambda_ingestion.config import SOURCE, ASSET_TYPE

def fetch_raw_crypto(symbol: str):
    """
    Fetch raw crypto event for Bronze layer.
    Lightweight placeholder for external API ingestion.
    """

    now = datetime.utcnow().isoformat()

    return {
        "symbol": symbol,
        "event_time": now,
        "ingested_at": now,
        "source": SOURCE,
        "asset_type": ASSET_TYPE
    }
