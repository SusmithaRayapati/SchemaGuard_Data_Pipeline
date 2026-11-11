
"""
SchemaGuard - Pipeline Orchestration Service

Responsibilities:
1. Schema detection and evolution
2. Record-level data validation
3. Compatible nullable column support
4. Late-arriving record detection
5. Data transformation
6. Batch and record quarantine
7. SQLite run tracking and metrics
8. Empty and malformed batch handling
9. Load transformed records into SQLite
"""

from pathlib import Path
import time
import uuid
import sqlite3

import pandas as pd
import pyarrow.parquet as pq

from backend.pipeline.schema_detector import (
    REGISTERED_SCHEMA,
    schema_fingerprint
)

from backend.pipeline.schema_validator import (
    validate_schema,
    validate_parquet_file
)

from backend.pipeline.transformation import (
    transform_records,
    save_transformed_data
)

from backend.pipeline.quarantine import (
    quarantine_batch,
    quarantine_records
)

from backend.database.connection import initialize_database

from backend.database.repository import (
    start_pipeline_run,
    record_batch,
    record_quarantine,
    record_metric,
    complete_pipeline_run,
    load_processed_orders
)


# ==================================================
# 1. PROJECT CONFIGURATION
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

INPUT_DIR = PROJECT_ROOT / "data" / "input"

CUSTOMER_FILE = INPUT_DIR / "customers.parquet"

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed_final_test"

ALLOWED_OPTIONAL_COLUMNS = {
    "region",
    "discount"
}

ALLOWED_LATENESS_DAYS = 2


# ==================================================
# 2. SCHEMA COMPATIBILITY CHECK
# ==================================================

def inspect_batch_schema(batch_file):
    """
    Compare incoming Parquet schema with registered schema.
    """

    try:
        incoming_schema = pq.read_schema(batch_file)

    except Exception as error:
        return {
            "status": "INVALID_INPUT",
            "schema": None,
            "fingerprint": None,
            "details": {
                "error": str(error)
            }
        }

    expected_fields = {
        field.name: field.type
        for field in REGISTERED_SCHEMA
    }

    incoming_fields = {
        field.name: field
        for field in incoming_schema
    }

    validation = validate_schema(incoming_schema)

    missing_columns = [
        name
        for name in expected_fields
        if name not in incoming_fields
    ]

    changed_columns = {}

    for name, expected_type in expected_fields.items():

        if name in incoming_fields:

            actual_type = incoming_fields[name].type

            if actual_type != expected_type:

                changed_columns[name] = {
                    "expected": str(expected_type),
                    "actual": str(actual_type)
                }

    new_optional_columns = [
        name
        for name in incoming_fields
        if name not in expected_fields
        and name in ALLOWED_OPTIONAL_COLUMNS
    ]

    unexpected_columns = [
        name
        for name in incoming_fields
        if name not in expected_fields
        and name not in ALLOWED_OPTIONAL_COLUMNS
    ]

    if not validation["is_valid"] or unexpected_columns:

        return {
            "status": "BREAKING",
            "schema": incoming_schema,
            "fingerprint": schema_fingerprint(incoming_schema),
            "details": {
                "missing_columns": missing_columns,
                "changed_columns": changed_columns,
                "unexpected_columns": unexpected_columns,
                "validation_errors": validation.get("errors", [])
            }
        }

    schema_status = (
        "COMPATIBLE"
        if new_optional_columns
        else "NO_CHANGE"
    )

    return {
        "status": schema_status,
        "schema": incoming_schema,
        "fingerprint": schema_fingerprint(incoming_schema),
        "details": {
            "new_optional_columns": new_optional_columns
        }
    }


# ==================================================
# 3. GET PREVIOUS EVENT-TIME WATERMARK
# ==================================================

def get_previous_event_time_watermark():
    """
    Find maximum order_date from previously processed
    Parquet files.
    """

    if not PROCESSED_DIR.exists():
        return None

    maximum_event_time = None

    for processed_file in PROCESSED_DIR.glob("*.parquet"):

        try:

            dates = pd.read_parquet(
                processed_file,
                columns=["order_date"]
            )

            event_dates = pd.to_datetime(
                dates["order_date"],
                utc=True,
                errors="coerce"
            ).dropna()

            if event_dates.empty:
                continue

            batch_maximum = event_dates.max()

            if (
                maximum_event_time is None
                or batch_maximum > maximum_event_time
            ):
                maximum_event_time = batch_maximum

        except Exception as error:

            print(
                f"Warning: Could not read watermark from "
                f"{processed_file.name}: {error}"
            )

    return maximum_event_time


# ==================================================
# 4. QUARANTINE ENTIRE BATCH
# ==================================================

def quarantine_entire_batch(
    batch_file,
    source_filename,
    run_id,
    schema_result,
    quarantine_reason
):
    """
    Quarantine a complete batch for breaking schema
    or invalid Parquet input.
    """

    schema = schema_result.get("schema")

    schema_info = {
        "actual_schema": (
            str(schema)
            if schema is not None
            else None
        )
    }

    quarantine_result = quarantine_batch(
        source_file=batch_file,
        run_id=run_id,
        quarantine_reason=quarantine_reason,
        error_details=schema_result["details"],
        schema_info=schema_info
    )

    record_batch(
        run_id=run_id,
        source_file=source_filename,
        schema_fingerprint=schema_result.get("fingerprint"),
        schema_status=schema_result["status"],
        batch_status="QUARANTINED",
        error_message=str(schema_result["details"])
    )

    manifest = quarantine_result["manifest"]

    record_quarantine(
        quarantine_id=quarantine_result["quarantine_id"],
        run_id=run_id,
        source_file=source_filename,
        quarantine_type="BATCH",
        quarantine_reason=quarantine_reason,
        record_count=manifest.get("record_count") or 0,
        artifact_path=quarantine_result["manifest_file"]
    )

    complete_pipeline_run(
        run_id=run_id,
        status="PARTIAL",
        accepted_files=0,
        quarantined_files=1
    )

    return {
        "run_id": run_id,
        "status": "PARTIAL",
        "source_file": source_filename,
        "schema_status": schema_result["status"],
        "message": "Batch quarantined.",
        "quarantine": quarantine_result
    }


# ==================================================
# 5. MAIN PIPELINE
# ==================================================

def run_pipeline(source_filename="batch_01.parquet"):
    """
    Execute the complete SchemaGuard pipeline.
    """

    initialize_database()

    # ----------------------------------------------
    # INPUT VALIDATION
    # ----------------------------------------------

    if Path(source_filename).name != source_filename:
        raise ValueError(
            "Only an input filename is accepted."
        )

    if not source_filename.endswith(".parquet"):
        raise ValueError(
            "Input must be a Parquet file."
        )

    batch_file = INPUT_DIR / source_filename

    if not batch_file.is_file():
        raise FileNotFoundError(
            f"Input batch not found: {batch_file}"
        )

    if not CUSTOMER_FILE.is_file():
        raise FileNotFoundError(
            f"Customer reference file not found: {CUSTOMER_FILE}"
        )

    # ----------------------------------------------
    # IDEMPOTENCY CHECK
    # ----------------------------------------------

    database_path = PROJECT_ROOT / "data" / "schemaguard.db"

    with sqlite3.connect(database_path) as connection:

        existing_batch = connection.execute(
            """
            SELECT run_id, batch_status, processed_file
            FROM batch_metadata
            WHERE source_file = ?
            AND batch_status IN (
                'SUCCESS',
                'SUCCESS_EMPTY',
                'QUARANTINED'
            )
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (source_filename,)
        ).fetchone()

    if existing_batch:

        print("\nBatch already processed.")
        print("Previous Run ID:", existing_batch[0])
        print("Previous Status:", existing_batch[1])

        return {
            "status": "SKIPPED_ALREADY_PROCESSED",
            "source_file": source_filename,
            "previous_run_id": existing_batch[0],
            "previous_status": existing_batch[1],
            "processed_file": existing_batch[2]
        }

    # ----------------------------------------------
    # CREATE PIPELINE RUN
    # ----------------------------------------------

    run_id = "SG_" + uuid.uuid4().hex[:12].upper()

    started = time.perf_counter()

    start_pipeline_run(
        run_id=run_id,
        total_files=1
    )

    print("\nStarting SchemaGuard pipeline...")
    print("Run ID:", run_id)
    print("Source:", source_filename)

    try:

        # ------------------------------------------
        # STEP 1: SCHEMA DETECTION
        # ------------------------------------------

        schema_result = inspect_batch_schema(batch_file)

        schema_status = schema_result["status"]

        fingerprint = schema_result["fingerprint"]

        print("\nSchema status:", schema_status)

        # ------------------------------------------
        # STEP 2: BATCH-LEVEL QUARANTINE
        # ------------------------------------------

        if schema_status == "BREAKING":

            return quarantine_entire_batch(
                batch_file,
                source_filename,
                run_id,
                schema_result,
                "BREAKING_SCHEMA_CHANGE"
            )

        if schema_status == "INVALID_INPUT":

            return quarantine_entire_batch(
                batch_file,
                source_filename,
                run_id,
                schema_result,
                "INVALID_PARQUET_INPUT"
            )

        # ------------------------------------------
        # STEP 3: PREVIOUS EVENT-TIME WATERMARK
        # ------------------------------------------

        print("\nChecking previous event-time watermark...")

        event_time_watermark = (
            get_previous_event_time_watermark()
        )

        print(
            "Previous watermark:",
            event_time_watermark
        )

        # ------------------------------------------
        # STEP 4: RECORD VALIDATION
        # ------------------------------------------

        print("\nValidating records...")

        validation_result = validate_parquet_file(
            batch_file,
            CUSTOMER_FILE,
            event_time_watermark=event_time_watermark,
            allowed_lateness_days=ALLOWED_LATENESS_DAYS
        )

        if validation_result["status"] not in (
            "SUCCESS",
            "SUCCESS_EMPTY"
        ):

            raise RuntimeError(
                "Record validation failed: "
                f"{validation_result.get('error', 'Unknown error')}"
            )

        valid_records = validation_result["valid_records"]

        rejected_records = validation_result["rejected_records"]

        summary = validation_result["summary"]

        input_count = summary["total_records"]

        valid_count = len(valid_records)

        rejected_count = len(rejected_records)

        late_count = summary.get("late_records", 0)

        print("Input records:", input_count)
        print("Valid records:", valid_count)
        print("Rejected records:", rejected_count)
        print("Late records:", late_count)

        # ------------------------------------------
        # STEP 5: TRANSFORMATION
        # ------------------------------------------

        print("\nTransforming valid records...")

        transformed_data = pd.DataFrame()

        output_file = None

        loaded_records = 0

        if valid_records.empty:

            print("No valid records available for transformation.")

        else:

            PROCESSED_DIR.mkdir(
                parents=True,
                exist_ok=True
            )

            output_file = (
                PROCESSED_DIR
                / f"{Path(source_filename).stem}_{run_id}.parquet"
            )

            transformed_data = transform_records(
                dataframe=valid_records,
                source_file=source_filename,
                run_id=run_id
            )

            save_transformed_data(
                transformed_data,
                output_file
            )

                    # ------------------------------------------
        # STEP 6: SQLITE DATA LOADING
        # ------------------------------------------

        print("\nLoading records into SQLite...")

        required_db_columns = {
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
        }

        if not transformed_data.empty:

            missing_db_columns = (
                required_db_columns
                - set(transformed_data.columns)
            )

            if missing_db_columns:

                print(
                    "Database loading skipped. "
                    "Transformed data is missing columns:",
                    sorted(missing_db_columns)
                )

            else:

                loaded_records = load_processed_orders(
                    transformed_data
                )

                print(
                    "Database records loaded:",
                    loaded_records
                )

        # ------------------------------------------
        # STEP 7: RECORD-LEVEL QUARANTINE
        # ------------------------------------------

        record_quarantine_result = None

        if not rejected_records.empty:

            record_quarantine_result = quarantine_records(
                rejected_dataframe=rejected_records,
                source_file=source_filename,
                run_id=run_id
            )

            record_quarantine(
                quarantine_id=(
                    record_quarantine_result["quarantine_id"]
                ),
                run_id=run_id,
                source_file=source_filename,
                quarantine_type="RECORD",
                quarantine_reason="DATA_QUALITY_FAILURE",
                record_count=rejected_count,
                artifact_path=(
                    record_quarantine_result["output_file"]
                )
            )

            print("\nRejected records quarantined.")

        # ------------------------------------------
        # STEP 8: BATCH METADATA
        # ------------------------------------------

        batch_status = (
            "SUCCESS_EMPTY"
            if input_count == 0
            else "SUCCESS"
        )

        record_batch(
            run_id=run_id,
            source_file=source_filename,
            schema_fingerprint=fingerprint,
            schema_status=schema_status,
            batch_status=batch_status,
            input_records=input_count,
            valid_records=valid_count,
            rejected_records=rejected_count,
            processed_file=(
                str(output_file)
                if output_file is not None
                else None
            )
        )

        # ------------------------------------------
        # STEP 9: PIPELINE METRICS
        # ------------------------------------------

        processing_time = time.perf_counter() - started

        metrics = {
            "processing_time": (
                processing_time,
                "seconds"
            ),
            "input_records": (
                input_count,
                "records"
            ),
            "output_records": (
                len(transformed_data),
                "records"
            ),
            "database_records": (
                loaded_records,
                "records"
            ),
            "rejected_records": (
                rejected_count,
                "records"
            ),
            "late_records": (
                late_count,
                "records"
            )
        }

        for metric_name, (metric_value, unit) in metrics.items():

            record_metric(
                run_id=run_id,
                metric_name=metric_name,
                metric_value=metric_value,
                unit=unit
            )

        # ------------------------------------------
        # STEP 10: COMPLETE PIPELINE RUN
        # ------------------------------------------

        final_status = (
            "PARTIAL"
            if rejected_count > 0
            else "SUCCESS"
        )

        complete_pipeline_run(
            run_id=run_id,
            status=final_status,
            accepted_files=1,
            quarantined_files=(
                1 if rejected_count > 0 else 0
            ),
            total_input_records=input_count,
            total_output_records=len(transformed_data),
            total_rejected_records=rejected_count
        )

        # ------------------------------------------
        # STEP 11: FINAL RESULT
        # ------------------------------------------

        result = {
            "run_id": run_id,
            "status": final_status,
            "source_file": source_filename,
            "schema_status": schema_status,
            "input_records": input_count,
            "valid_records": valid_count,
            "output_records": len(transformed_data),
            "database_records": loaded_records,
            "rejected_records": rejected_count,
            "late_records": late_count,
            "processing_time_seconds": round(
                processing_time,
                4
            ),
            "processed_file": (
                str(output_file)
                if output_file is not None
                else None
            )
        }

        print("\nPipeline completed.")
        print(result)

        return result

    except Exception as error:

        try:

            complete_pipeline_run(
                run_id=run_id,
                status="FAILED",
                error_message=str(error)
            )

        except Exception as tracking_error:

            print(
                "Could not update failed run:",
                tracking_error
            )

        raise


# ==================================================
# 6. DIRECT EXECUTION
# ==================================================

if __name__ == "__main__":

    run_pipeline("batch_01.parquet")
