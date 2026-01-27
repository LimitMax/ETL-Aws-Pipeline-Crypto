def validate_silver(records):
    if not records:
        raise ValueError("Silver dataset is empty")

    required = {"symbol", "datex", "hourx", "open", "close"}
    for r in records:
        missing = required - r.keys()
        if missing:
            raise ValueError(f"Missing fields: {missing}")
