from ingestion.market.batch import fetch_all_data

data = fetch_all_data()

print(f"Total records: {len(data)}")
print(data[:2])