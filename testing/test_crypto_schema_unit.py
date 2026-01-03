def test_crypto_schema_unit():
    dummy_record = {
        "datex": "2026-01-01",
        "hourx": 10,
        "symbol": "BTC-USD",
        "open": 43000.0,
        "high": 43500.0,
        "low": 42800.0,
        "close": 43200.0,
        "volume": 123456.0,
        "source": "yfinance",
        "asset_type": "crypto"
    }

    expected_keys = {
        "datex", "hourx", "symbol",
        "open", "high", "low", "close",
        "volume", "source", "asset_type"
    }

    assert set(dummy_record.keys()) == expected_keys
