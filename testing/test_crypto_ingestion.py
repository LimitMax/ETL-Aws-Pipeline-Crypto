# tests/test_crypto_ingestion.py

from ingestion.market.logic import fetch_crypto_data


def test_crypto_schema():
    data = fetch_crypto_data("BTC-USD", "1h", "1d")
    sample = data[0]

    expected_keys = {
        "datex", "hourx", "symbol",
        "open", "high", "low", "close",
        "volume", "source", "asset_type"
    }

    assert set(sample.keys()) == expected_keys
