
"""
SchemaGuard - Database Repository

Responsibilities:
1. Start pipeline runs.
2. Store batch processing information.
3. Store quarantine information.
4. Store pipeline metrics.
5. Complete or fail pipeline runs.
6. Retrieve historical pipeline information.
7. Load transformed orders into SQLite.
8. Retrieve processed orders.
"""

from datetime import datetime, timezone

import pandas as pd

from backend.database.connection import (
    database_session,
    initialize_database
)


# --------------------------------------------------
# 1. COMMON HELPER
# --------------------------------------------------

def get_utc_timestamp():
    return datetime.now(timezone.utc).isoformat()


def row_to_dict(row):
    if row is None:
        return None
    return dict(row)


# --------------------------------------------------
# 2. START PIPELINE RUN
# --------------------------------------------------

def start_pipeline_run(run_id, total_files=0):

    if not run_id:
        raise ValueError("run_id cannot be empty")

    if total_files < 0:
        raise ValueError("total_files cannot be negative")

    started_at = get_utc_timestamp()

    with database_session() as connection:
        connection.execute(
            """
            INSERT INTO pipeline_runs (
                run_id, started_at, status, total_files
            )
            VALUES (?, ?, ?, ?)
            """,
            (run_id, started_at, "RUNNING", total_files)
        )

    return {
        "run_id": run_id,
        "status": "RUNNING",
        "started_at": started_at
    }


# --------------------------------------------------
# 3. STORE BATCH METADATA
# --------------------------------------------------

def record_batch(
    run_id,
    source_file,
    batch_status,
    schema_fingerprint=None,
    schema_status=None,
    input_records=0,
    valid_records=0,
    rejected_records=0,
    processed_file=None,
    error_message=None
):

    created_at = get_utc_timestamp()

    with database_session() as connection:
        connection.execute(
            """
            INSERT INTO batch_metadata (
                run_id, source_file, schema_fingerprint,
                schema_status, batch_status, input_records,
                valid_records, rejected_records, processed_file,
                error_message, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

            ON CONFLICT(run_id, source_file)
            DO UPDATE SET
                schema_fingerprint = excluded.schema_fingerprint,
                schema_status = excluded.schema_status,
                batch_status = excluded.batch_status,
                input_records = excluded.input_records,
                valid_records = excluded.valid_records,
                rejected_records = excluded.rejected_records,
                processed_file = excluded.processed_file,
                error_message = excluded.error_message,
                created_at = excluded.created_at
            """,
            (
                run_id, source_file, schema_fingerprint,
                schema_status, batch_status, input_records,
                valid_records, rejected_records, processed_file,
                error_message, created_at
            )
        )

    return {
        "status": "SAVED",
        "source_file": source_file,
        "run_id": run_id
    }


# --------------------------------------------------
# 4. STORE QUARANTINE INFORMATION
# --------------------------------------------------

def record_quarantine(
    quarantine_id,
    run_id,
    source_file,
    quarantine_type,
    quarantine_reason,
    record_count=0,
    artifact_path=None
):

    created_at = get_utc_timestamp()

    with database_session() as connection:
        connection.execute(
            """
            INSERT INTO quarantine_logs (
                quarantine_id, run_id, source_file,
                quarantine_type, quarantine_reason,
                record_count, artifact_path, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                quarantine_id, run_id, source_file,
                quarantine_type, quarantine_reason,
                record_count,
                str(artifact_path) if artifact_path else None,
                created_at
            )
        )

    return {
        "status": "SAVED",
        "quarantine_id": quarantine_id
    }


# --------------------------------------------------
# 5. STORE PIPELINE METRICS
# --------------------------------------------------

def record_metric(run_id, metric_name, metric_value, unit=None):

    if metric_value is None:
        raise ValueError("metric_value cannot be None")

    recorded_at = get_utc_timestamp()

    with database_session() as connection:
        connection.execute(
            """
            INSERT INTO pipeline_metrics (
                run_id, metric_name, metric_value,
                unit, recorded_at
            )
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                run_id, metric_name,
                float(metric_value), unit, recorded_at
            )
        )

    return {
        "status": "SAVED",
        "metric_name": metric_name
    }


# --------------------------------------------------
# 6. LOAD TRANSFORMED DATA INTO SQLITE
# --------------------------------------------------

def load_processed_orders(df):
    """
    Bulk load transformed records into processed_orders.

    Existing order IDs are updated during retries.
    """

    if not isinstance(df, pd.DataFrame):
        raise TypeError("Input must be a pandas DataFrame")

    if df.empty:
        return 0

    columns = [
        "order_id",
        "customer_id",
        "order_date",
        "amount",
        "status",
        "region",
        "discount",
        "discount_amount",
        "net_amount",
        "order_year",
        "order_month",
        "source_file",
        "run_id"
    ]

    missing = set(columns) - set(df.columns)

    if missing:
        raise ValueError(
            f"Missing columns: {sorted(missing)}"
        )

    records = df[columns].copy()

    records["order_date"] = pd.to_datetime(
        records["order_date"],
        utc=True
    ).astype(str)

    records = records.astype(object).where(
        pd.notna(records),
        None
    )

    sql = """
    INSERT INTO processed_orders (
        order_id, customer_id, order_date, amount,
        status, region, discount, discount_amount,
        net_amount, order_year, order_month,
        source_file, run_id
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

    ON CONFLICT(order_id) DO UPDATE SET
        customer_id = excluded.customer_id,
        order_date = excluded.order_date,
        amount = excluded.amount,
        status = excluded.status,
        region = excluded.region,
        discount = excluded.discount,
        discount_amount = excluded.discount_amount,
        net_amount = excluded.net_amount,
        order_year = excluded.order_year,
        order_month = excluded.order_month,
        source_file = excluded.source_file,
        run_id = excluded.run_id;
    """

    with database_session() as connection:
        connection.executemany(
            sql,
            records.itertuples(index=False, name=None)
        )

    return len(records)


# --------------------------------------------------
# 7. RETRIEVE PROCESSED ORDERS
# --------------------------------------------------

def get_processed_orders(limit=100):

    limit = max(1, min(int(limit), 1000))

    with database_session() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM processed_orders
            ORDER BY loaded_at DESC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()

    return [row_to_dict(row) for row in rows]


# --------------------------------------------------
# 8. COMPLETE PIPELINE RUN
# --------------------------------------------------

def complete_pipeline_run(
    run_id,
    status,
    accepted_files=0,
    quarantined_files=0,
    total_input_records=0,
    total_output_records=0,
    total_rejected_records=0,
    error_message=None
):

    allowed_statuses = ["SUCCESS", "FAILED", "PARTIAL"]

    if status not in allowed_statuses:
        raise ValueError(f"Invalid final status: {status}")

    completed_at = get_utc_timestamp()

    with database_session() as connection:
        cursor = connection.execute(
            """
            UPDATE pipeline_runs
            SET
                completed_at = ?,
                status = ?,
                accepted_files = ?,
                quarantined_files = ?,
                total_input_records = ?,
                total_output_records = ?,
                total_rejected_records = ?,
                error_message = ?
            WHERE run_id = ?
            AND status = 'RUNNING'
            """,
            (
                completed_at, status, accepted_files,
                quarantined_files, total_input_records,
                total_output_records, total_rejected_records,
                error_message, run_id
            )
        )

        if cursor.rowcount == 0:
            raise ValueError(
                f"Active pipeline run not found: {run_id}"
            )

    return {
        "run_id": run_id,
        "status": status,
        "completed_at": completed_at
    }


# --------------------------------------------------
# 9. GET PIPELINE RUN
# --------------------------------------------------

def get_pipeline_run(run_id):

    with database_session() as connection:
        row = connection.execute(
            """
            SELECT *
            FROM pipeline_runs
            WHERE run_id = ?
            """,
            (run_id,)
        ).fetchone()

    return row_to_dict(row)


# --------------------------------------------------
# 10. GET ALL PIPELINE RUNS
# --------------------------------------------------

def get_all_pipeline_runs(limit=20):

    limit = max(1, min(int(limit), 500))

    with database_session() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM pipeline_runs
            ORDER BY started_at DESC
            LIMIT ?
            """,
            (limit,)
        ).fetchall()

    return [row_to_dict(row) for row in rows]


# --------------------------------------------------
# 11. GET BATCH DETAILS
# --------------------------------------------------

def get_batches_for_run(run_id):

    with database_session() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM batch_metadata
            WHERE run_id = ?
            ORDER BY id
            """,
            (run_id,)
        ).fetchall()

    return [row_to_dict(row) for row in rows]


# --------------------------------------------------
# 12. GET QUARANTINE HISTORY
# --------------------------------------------------

def get_quarantine_logs(run_id=None):

    with database_session() as connection:

        if run_id:
            rows = connection.execute(
                """
                SELECT *
                FROM quarantine_logs
                WHERE run_id = ?
                ORDER BY created_at DESC
                """,
                (run_id,)
            ).fetchall()

        else:
            rows = connection.execute(
                """
                SELECT *
                FROM quarantine_logs
                ORDER BY created_at DESC
                """
            ).fetchall()

    return [row_to_dict(row) for row in rows]


# --------------------------------------------------
# 13. GET PIPELINE METRICS
# --------------------------------------------------

def get_metrics_for_run(run_id):

    with database_session() as connection:
        rows = connection.execute(
            """
            SELECT *
            FROM pipeline_metrics
            WHERE run_id = ?
            ORDER BY id
            """,
            (run_id,)
        ).fetchall()

    return [row_to_dict(row) for row in rows]


# --------------------------------------------------
# 14. DATABASE INTEGRATION TEST
# --------------------------------------------------

def test_database_repository():

    import uuid

    initialize_database()

    run_id = "SCHEMAGUARD_DB_TEST_" + uuid.uuid4().hex[:8]

    print("\nStarting database integration test...")

    start_pipeline_run(run_id, total_files=1)

    record_batch(
        run_id=run_id,
        source_file="batch_01.parquet",
        batch_status="SUCCESS",
        schema_status="NO_CHANGE",
        input_records=106,
        valid_records=100,
        rejected_records=6
    )

    record_quarantine(
        quarantine_id=uuid.uuid4().hex[:12],
        run_id=run_id,
        source_file="batch_01.parquet",
        quarantine_type="RECORD",
        quarantine_reason="DATA_QUALITY_FAILURE",
        record_count=6
    )

    record_metric(
        run_id=run_id,
        metric_name="processing_time",
        metric_value=2.5,
        unit="seconds"
    )

    complete_pipeline_run(
        run_id=run_id,
        status="SUCCESS",
        accepted_files=1,
        total_input_records=106,
        total_output_records=100,
        total_rejected_records=6
    )

    print("\nPipeline run:", get_pipeline_run(run_id))
    print("\nBatches:", get_batches_for_run(run_id))
    print("\nQuarantine:", get_quarantine_logs(run_id))
    print("\nMetrics:", get_metrics_for_run(run_id))

    print("\nDatabase integration test completed.")


# --------------------------------------------------
# 15. DIRECT EXECUTION
# --------------------------------------------------

if __name__ == "__main__":
    test_database_repository()
