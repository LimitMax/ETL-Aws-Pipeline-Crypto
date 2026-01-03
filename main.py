from ingestion.market.batch import fetch_all_data


def main():
    print("[INFO] Running local crypto ingestion pipeline...")

    data = fetch_all_data()

    print(f"[INFO] Total records fetched: {len(data)}")

    if data:
        print("[INFO] Sample records:")
        for record in data[:2]:
            print(record)


if __name__ == "__main__":
    main()
