def validate_gold(df, expected_symbols: int | None = None):
    errors = []

    if df is None or df.empty:
        errors.append("Gold dataset is empty")

    required_cols = {
        "dt",
        "symbol",
        "open_daily",
        "high_daily",
        "low_daily",
        "close_daily",
        "volume_daily",
        "source",
        "asset_type",
    }

    missing = required_cols - set(df.columns)
    if missing:
        errors.append(f"Missing columns: {missing}")

    # Sanity checks
    if not df.empty:
        if (df["high_daily"] < df["low_daily"]).any():
            errors.append("Invalid price range: high_daily < low_daily detected")

        if (df["volume_daily"] < 0).any():
            errors.append("Negative volume_daily detected")

        if df["symbol"].isnull().any():
            errors.append("Null symbol detected")

        if df["close_daily"].isnull().any():
            errors.append("Null close_daily detected")

    # Completeness check per day (optional, but powerful)
    if expected_symbols and not df.empty:
        counts = df.groupby("dt")["symbol"].nunique()
        for dt, cnt in counts.items():
            if cnt < expected_symbols:
                errors.append(
                    f"Incomplete symbols for dt={dt}: {cnt}/{expected_symbols}"
                )

    if errors:
        raise ValueError("Gold data quality failed: " + " | ".join(errors))
