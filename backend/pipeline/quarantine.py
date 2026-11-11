
"""
SchemaGuard - Quarantine Management

Responsibilities:
1. Quarantine complete batches with breaking schemas.
2. Quarantine individual rejected records.
3. Preserve rejection reasons and source metadata.
4. Generate JSON manifests for quarantined batches.
5. Save rejected records as Parquet files.
6. Avoid overwriting previous quarantine files.
7. Use atomic file writes.
"""

from pathlib import Path
from datetime import datetime, timezone
import json
import re
import uuid

import pandas as pd


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

QUARANTINE_DIR = PROJECT_ROOT / "data" / "quarantine"

BATCH_QUARANTINE_DIR = QUARANTINE_DIR / "batches"

RECORD_QUARANTINE_DIR = QUARANTINE_DIR / "records"

BATCH_QUARANTINE_DIR.mkdir(
    parents=True,
    exist_ok=True
)

RECORD_QUARANTINE_DIR.mkdir(
    parents=True,
    exist_ok=True
)


# --------------------------------------------------
# 2. COMMON HELPER FUNCTIONS
# --------------------------------------------------

def get_utc_timestamp():
    """Return current UTC timestamp in ISO-8601 format."""

    return datetime.now(timezone.utc).isoformat()


def sanitize_filename(filename):
    """
    Remove unsafe characters from filenames.
    """

    filename = Path(str(filename)).name

    filename = re.sub(
        r"[^a-zA-Z0-9_.-]",
        "_",
        filename
    )

    return filename or "unknown_file"


def generate_quarantine_id():
    """Generate a unique quarantine identifier."""

    return uuid.uuid4().hex[:12]


def make_json_serializable(value):
    """
    Convert common Python and Pandas values into
    JSON-compatible values.
    """

    if isinstance(value, dict):
        return {
            str(key): make_json_serializable(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [
            make_json_serializable(item)
            for item in value
        ]

    if isinstance(value, Path):
        return str(value)

    if hasattr(value, "isoformat"):
        return value.isoformat()

    if hasattr(value, "item"):
        return value.item()

    if pd.isna(value):
        return None

    return value


def atomic_write_json(data, output_file):
    """
    Write JSON through a temporary file and then
    replace the final file.
    """

    output_file = Path(output_file)

    temp_file = output_file.with_name(
        output_file.stem
        + "."
        + uuid.uuid4().hex
        + ".tmp"
    )

    try:
        with open(
            temp_file,
            "w",
            encoding="utf-8"
        ) as file:

            json.dump(
                make_json_serializable(data),
                file,
                indent=4,
                ensure_ascii=False
            )

        temp_file.replace(output_file)

    finally:
        if temp_file.exists():
            temp_file.unlink()

    return output_file


def atomic_write_parquet(dataframe, output_file):
    """
    Write Parquet through a temporary file.
    """

    output_file = Path(output_file)

    temp_file = output_file.with_name(
        output_file.stem
        + "."
        + uuid.uuid4().hex
        + ".tmp.parquet"
    )

    try:
        dataframe.to_parquet(
            temp_file,
            index=False,
            engine="pyarrow",
            compression="snappy"
        )

        temp_file.replace(output_file)

    finally:
        if temp_file.exists():
            temp_file.unlink()

    return output_file


# --------------------------------------------------
# 3. BATCH-LEVEL QUARANTINE
# --------------------------------------------------

def quarantine_batch(
    source_file,
    run_id,
    quarantine_reason,
    error_details=None,
    schema_info=None
):
    """
    Quarantine an entire source batch.

    Used when:
    - Required columns are removed.
    - Required datatypes change unexpectedly.
    - Parquet file is corrupted.
    - Schema evolution is breaking.
    """

    source_file = Path(source_file)

    if not quarantine_reason:
        raise ValueError(
            "quarantine_reason cannot be empty"
        )

    if not run_id:
        raise ValueError(
            "run_id cannot be empty"
        )

    quarantine_id = generate_quarantine_id()

    safe_filename = sanitize_filename(
        source_file.name
    )

    manifest = {
        "quarantine_id": quarantine_id,
        "quarantine_type": "BATCH",
        "source_file": safe_filename,
        "source_path": str(source_file),
        "run_id": str(run_id),
        "quarantined_at": get_utc_timestamp(),
        "status": "QUARANTINED",
        "quarantine_reason": str(quarantine_reason),
        "error_details": error_details or {},
        "schema_info": schema_info or {},
        "record_count": None
    }

    # Try to capture record count when the file is readable.
    if source_file.exists():

        try:
            manifest["record_count"] = len(
                pd.read_parquet(
                    source_file,
                    engine="pyarrow"
                )
            )

        except Exception as error:

            manifest["record_count_error"] = str(error)

    manifest_file = (
        BATCH_QUARANTINE_DIR
        / f"{safe_filename}_{quarantine_id}.json"
    )

    atomic_write_json(
        manifest,
        manifest_file
    )

    print("\nBatch quarantined successfully.")

    print(f"Source file: {safe_filename}")

    print(f"Reason: {quarantine_reason}")

    print(f"Manifest: {manifest_file}")

    return {
        "status": "QUARANTINED",
        "quarantine_id": quarantine_id,
        "manifest_file": manifest_file,
        "manifest": manifest
    }


# --------------------------------------------------
# 4. RECORD-LEVEL QUARANTINE
# --------------------------------------------------

def quarantine_records(
    rejected_dataframe,
    source_file,
    run_id,
    quarantine_reason="DATA_QUALITY_FAILURE"
):
    """
    Quarantine individual rejected records.

    Original columns and rejection_reasons are preserved.
    Additional traceability metadata is added.
    """

    if not isinstance(
        rejected_dataframe,
        pd.DataFrame
    ):
        raise TypeError(
            "rejected_dataframe must be a Pandas DataFrame"
        )

    if rejected_dataframe.empty:

        return {
            "status": "NO_REJECTED_RECORDS",
            "record_count": 0,
            "output_file": None
        }

    if not source_file:
        raise ValueError(
            "source_file cannot be empty"
        )

    if not run_id:
        raise ValueError(
            "run_id cannot be empty"
        )

    df = rejected_dataframe.copy()

    quarantine_id = generate_quarantine_id()

    quarantined_at = get_utc_timestamp()

    safe_filename = sanitize_filename(source_file)

    # Add record-level metadata.
    df["source_file"] = safe_filename

    df["run_id"] = str(run_id)

    df["quarantined_at"] = quarantined_at

    df["quarantine_type"] = "RECORD"

    # Add a general reason if rejection_reasons is absent.
    if "rejection_reasons" not in df.columns:

        df["rejection_reasons"] = quarantine_reason

    output_file = (
        RECORD_QUARANTINE_DIR
        / f"{Path(safe_filename).stem}_{quarantine_id}.parquet"
    )

    atomic_write_parquet(
        df,
        output_file
    )

    result = {
        "status": "QUARANTINED",
        "quarantine_id": quarantine_id,
        "source_file": safe_filename,
        "run_id": str(run_id),
        "quarantined_at": quarantined_at,
        "quarantine_reason": quarantine_reason,
        "record_count": len(df),
        "output_file": output_file
    }

    print("\nRejected records quarantined successfully.")

    print(f"Source file: {safe_filename}")

    print(f"Rejected records: {len(df)}")

    print(f"Output file: {output_file}")

    return result


# --------------------------------------------------
# 5. LOAD BATCH QUARANTINE MANIFEST
# --------------------------------------------------

def load_quarantine_manifest(manifest_file):
    """
    Read a previously generated batch quarantine manifest.
    """

    manifest_file = Path(manifest_file)

    if not manifest_file.exists():
        raise FileNotFoundError(
            f"Manifest not found: {manifest_file}"
        )

    with open(
        manifest_file,
        "r",
        encoding="utf-8"
    ) as file:

        return json.load(file)


# --------------------------------------------------
# 6. DISPLAY QUARANTINE REPORT
# --------------------------------------------------

def print_quarantine_report(
    batch_result=None,
    record_result=None
):

    print("\n" + "=" * 55)

    print("SCHEMAGUARD - QUARANTINE REPORT")

    print("=" * 55)

    if batch_result:

        print("\nBATCH-LEVEL QUARANTINE")

        print(
            f"Status: {batch_result['status']}"
        )

        print(
            f"Quarantine ID: {batch_result['quarantine_id']}"
        )

        print(
            f"Manifest: {batch_result['manifest_file']}"
        )

    if record_result:

        print("\nRECORD-LEVEL QUARANTINE")

        print(
            f"Status: {record_result['status']}"
        )

        print(
            f"Rejected records: {record_result['record_count']}"
        )

        print(
            f"Output: {record_result['output_file']}"
        )

    print("\nQuarantine directories:")

    print(f"Batch quarantine: {BATCH_QUARANTINE_DIR}")

    print(f"Record quarantine: {RECORD_QUARANTINE_DIR}")

    print("=" * 55)


# --------------------------------------------------
# 7. TEST RECORD-LEVEL QUARANTINE USING BATCH 01
# --------------------------------------------------

def test_record_quarantine():

    from backend.pipeline.schema_validator import (
        validate_parquet_file
    )

    input_directory = PROJECT_ROOT / "data" / "input"

    batch_file = input_directory / "batch_01.parquet"

    customer_file = input_directory / "customers.parquet"

    validation_result = validate_parquet_file(
        batch_file,
        customer_file
    )

    if validation_result["status"] != "SUCCESS":

        raise RuntimeError(
            "Batch validation failed: "
            f"{validation_result['error']}"
        )

    rejected_records = validation_result["rejected_records"]

    result = quarantine_records(
        rejected_dataframe=rejected_records,
        source_file=batch_file.name,
        run_id="SCHEMAGUARD_QUARANTINE_TEST_001"
    )

    return result


# --------------------------------------------------
# 8. TEST BATCH-LEVEL QUARANTINE USING BATCH 04
# --------------------------------------------------

def test_batch_quarantine():

    from backend.pipeline.schema_detector import (
        read_parquet_schema,
        schema_to_dict
    )

    input_directory = PROJECT_ROOT / "data" / "input"

    batch_file = input_directory / "batch_04.parquet"

    batch_schema = read_parquet_schema(batch_file)

    result = quarantine_batch(
        source_file=batch_file,
        run_id="SCHEMAGUARD_QUARANTINE_TEST_002",
        quarantine_reason="BREAKING_SCHEMA_CHANGE",
        error_details={
            "column": "order_id",
            "expected_type": "int64",
            "actual_type": "string"
        },
        schema_info=schema_to_dict(batch_schema)
    )

    return result


# --------------------------------------------------
# 9. DIRECT EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    record_result = test_record_quarantine()

    batch_result = test_batch_quarantine()

    print_quarantine_report(
        batch_result=batch_result,
        record_result=record_result
    )
