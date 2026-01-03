from ingestion.market.batch import fetch_all_data


def lambda_handler(event, context):
    """
    AWS Lambda entry point for hourly crypto ingestion.
    """
    records = fetch_all_data()

    return {
        "status": "success",
        "records_count": len(records)
    }
