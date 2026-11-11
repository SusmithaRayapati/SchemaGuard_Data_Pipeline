
"""
SchemaGuard - Late Arrival Detection

Responsibilities:
1. Detect late-arriving records.
2. Keep valid late records.
3. Separate late records for tracking.
4. Add late-arrival metadata.
"""

import pandas as pd


def detect_late_records(
    dataframe,
    date_column="order_date",
    threshold_days=2,
    processing_time=None
):
    """
    Identify records arriving later than the configured threshold.

    Records older than threshold_days are marked as late.
    Late records are not rejected.
    """

    if dataframe.empty:
        return dataframe.copy(), dataframe.copy()

    if date_column not in dataframe.columns:
        raise ValueError(
            f"Missing date column: {date_column}"
        )

    result = dataframe.copy()

    if processing_time is None:
        processing_time = pd.Timestamp.now(tz="UTC")

    processing_time = pd.Timestamp(processing_time)

    if processing_time.tzinfo is None:
        processing_time = processing_time.tz_localize("UTC")
    else:
        processing_time = processing_time.tz_convert("UTC")

    event_dates = pd.to_datetime(
        result[date_column],
        utc=True,
        errors="coerce"
    )

    age = processing_time - event_dates

    result["is_late"] = (
        event_dates.notna()
        & (age > pd.Timedelta(days=threshold_days))
    )

    result["arrival_delay_days"] = (
        age.dt.total_seconds() / 86400
    ).where(event_dates.notna())

    late_records = result[
        result["is_late"]
    ].copy()

    accepted_records = result.copy()

    return accepted_records, late_records


def test_late_arrival_detection():

    sample = pd.DataFrame({
        "order_id": [1, 2, 3],
        "order_date": [
            "2026-11-10T10:00:00Z",
            "2026-11-05T10:00:00Z",
            "2026-11-11T08:00:00Z"
        ]
    })

    accepted, late = detect_late_records(
        sample,
        threshold_days=2,
        processing_time="2026-11-11T10:00:00Z"
    )

    assert len(accepted) == 3
    assert len(late) == 1
    assert late.iloc[0]["order_id"] == 2

    print("Late arrival detection test passed.")
    print("Total records:", len(accepted))
    print("Late records:", len(late))


if __name__ == "__main__":
    test_late_arrival_detection()
