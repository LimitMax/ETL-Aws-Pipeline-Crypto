import os
from datetime import datetime, timezone

from silver.reader import read_bronze_objects
from silver.transformer import bronze_to_silver
from silver.writer import write_silver
from silver.quality import validate_silver
from silver.config import (
    BRONZE_BUCKET,
    SILVER_BUCKET,
    BRONZE_PREFIX,
    SILVER_PREFIX,
)


def _get_runtime_window():
    """
    Determine dt/hour window for Silver job.

    Priority:
    1. Explicit ENV (DT, HOUR) -> rerun / backfill
    2. Current UTC hour -> scheduled 2-hour run
    """

    dt = os.environ.get("DT")
    hour = os.environ.get("HOUR")

    if dt and hour:
        return dt, hour

    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%d"), now.strftime("%H")


def main():
    if not BRONZE_BUCKET or not SILVER_BUCKET:
        raise RuntimeError("BRONZE_BUCKET and SILVER_BUCKET must be set")

    dt, hour = _get_runtime_window()

    print(f"[INFO] Silver job started dt={dt} hour={hour}")

    # ----------------------
    # Read Bronze (scoped)
    # ----------------------
    bronze_records = read_bronze_objects(
        bucket=BRONZE_BUCKET,
        base_prefix=BRONZE_PREFIX.split("/asset_type=")[0],
        dt=dt,
        hour=hour,
    )

    if not bronze_records:
        print(
            f"[WARN] No Bronze records found for dt={dt} hour={hour}. "
            f"Skipping Silver write."
        )
        return

    print(f"[INFO] Bronze records loaded: {len(bronze_records)}")

    # ----------------------
    # Transform → Silver
    # ----------------------
    silver_records = bronze_to_silver(bronze_records)

    # ----------------------
    # Quality Gate
    # ----------------------
    validate_silver(silver_records)

    # ----------------------
    # Write Silver
    # ----------------------
    write_silver(
        records=silver_records,
        bucket=SILVER_BUCKET,
        prefix=SILVER_PREFIX,
    )

    print(
        f"[SUCCESS] Silver job completed "
        f"dt={dt} hour={hour} rows={len(silver_records)}"
    )


if __name__ == "__main__":
    main()
