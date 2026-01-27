from datetime import datetime
from typing import List, Dict


def bronze_to_silver(bronze_records: List[Dict]) -> List[Dict]:
    """
    Transform Bronze records into Silver records.

    Assumptions (STRICT):
    - Bronze schema is valid and consistent
    - event_time is ISO8601 UTC string
    - No deduplication here (handled downstream if needed)

    Silver responsibilities:
    - derive datex, hourx
    - cast numeric fields
    - preserve domain fields
    """

    silver_records: List[Dict] = []

    for r in bronze_records:
        try:
            event_dt = datetime.fromisoformat(r["event_time"])

            silver_records.append({
                "event_time": r["event_time"],
                "datex": event_dt.strftime("%Y-%m-%d"),
                "hourx": event_dt.strftime("%H"),
                "symbol": r["symbol"],
                "asset_type": r["asset_type"],
                "source": r["source"],
                "open": float(r["open"]),
                "high": float(r["high"]),
                "low": float(r["low"]),
                "close": float(r["close"]),
                "volume": float(r["volume"]),
            })

        except KeyError as exc:
            raise ValueError(
                f"Missing required Bronze field: {exc} in record={r}"
            )

        except Exception as exc:
            raise ValueError(
                f"Failed to transform Bronze → Silver. "
                f"record={r} error={exc}"
            )

    return silver_records
