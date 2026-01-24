from typing import List, Dict

def filter_date_range(
    records: List[Dict],
    start_date: str,
    end_date: str
):
    """
    Filter records within [start_date, end_date].
    """
    return [
        r for r in records
        if start_date <= r["datex"] <= end_date
    ]
