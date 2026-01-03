from ingestion.market.logic import fetch_data_crypto

def test_crypto_schema_integration():
    data = fetch_data_crypto("BTC-USD", "1h", "1d")

    assert isinstance(data, list)
    assert len(data) > 0

    sample = data[0]

    expected_keys = {
        "datex", "hourx", "symbol",
        "open", "high", "low", "close",
        "volume", "source", "asset_type"
    }

    assert set(sample.keys()) == expected_keys
