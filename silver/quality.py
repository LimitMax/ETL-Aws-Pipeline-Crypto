def validate_silver(records: list):
    """
    Data quality checks for Silver layer.
    Fail fast if critical data issues are detected.
    """
    if not records:
        raise ValueError("Silver dataset is empty")

    required_keys = {
        "datex", "hourx", "symbol",
        "open", "high", "low", "close",
        "volume", "source", "asset_type"
    }

    errors = []

    for r in records:
        missing = required_keys - r.keys()
        if missing:
            errors.append(f"Missing keys: {missing}")
            break

        if r["symbol"] is None:
            errors.append("Null symbol detected")
            break

        if r["close"] is None:
            errors.append("Null close price detected")
            break

        if r["volume"] < 0:
            errors.append("Negative volume detected")
            break

        if r["high"] < r["low"]:
            errors.append("High price lower than low price detected")
            break

    if errors:
        raise ValueError("Silver data quality failed: " + " | ".join(errors))
