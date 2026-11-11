
"""
SchemaGuard - Schema Detection Engine

Responsibilities:
1. Read incoming Parquet schema.
2. Generate deterministic schema fingerprints.
3. Compare incoming schema with registered schema.
4. Detect added, removed and modified columns.
5. Identify compatible and breaking changes.
6. Return a structured detection report.

This module detects schema changes.
It does not approve or transform incoming data.
"""

import hashlib
import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


# --------------------------------------------------
# 1. REGISTERED BASELINE SCHEMA
# --------------------------------------------------

REGISTERED_SCHEMA = pa.schema([
    pa.field("order_id", pa.int64()),
    pa.field("customer_id", pa.int64()),
    pa.field("order_date", pa.timestamp("us", tz="UTC")),
    pa.field("amount", pa.float64()),
    pa.field("status", pa.string())
])

# These columns must always exist.
REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "amount",
    "status"
]


# --------------------------------------------------
# 2. GENERATE SCHEMA FINGERPRINT
# --------------------------------------------------

def schema_fingerprint(schema):
    """
    Generate a deterministic SHA-256 fingerprint.

    The fingerprint depends on:
    - Column names
    - Data types
    - Nullability

    Identical schemas produce identical fingerprints.
    """

    schema_definition = []

    for field in schema:

        schema_definition.append({
            "name": field.name,
            "type": str(field.type),
            "nullable": field.nullable
        })

    schema_json = json.dumps(
        schema_definition,
        sort_keys=True
    )

    fingerprint = hashlib.sha256(
        schema_json.encode("utf-8")
    ).hexdigest()

    return fingerprint


# --------------------------------------------------
# 3. READ PARQUET SCHEMA
# --------------------------------------------------

def read_parquet_schema(file_path):
    """
    Read schema metadata without loading all records.

    Raises an exception when the Parquet file is invalid
    or cannot be accessed.
    """

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(
            f"File does not exist: {file_path}"
        )

    schema = pq.read_schema(file_path)

    return schema


# --------------------------------------------------
# 4. CONVERT SCHEMA INTO DICTIONARY
# --------------------------------------------------

def schema_to_dict(schema):
    """
    Convert PyArrow schema into a JSON-friendly structure.
    """

    columns = []

    for field in schema:

        columns.append({
            "column_name": field.name,
            "data_type": str(field.type),
            "nullable": field.nullable
        })

    return {
        "columns": columns,
        "column_count": len(columns),
        "fingerprint": schema_fingerprint(schema)
    }


# --------------------------------------------------
# 5. COMPARE TWO SCHEMAS
# --------------------------------------------------

def compare_schemas(
    registered_schema,
    incoming_schema,
    required_columns=None
):
    """
    Compare incoming schema against registered schema.

    Classification:

    NO_CHANGE:
        No schema differences.

    COMPATIBLE:
        Only nullable columns have been added.

    BREAKING:
        Required columns removed, data types changed,
        or non-nullable columns added.
    """

    if required_columns is None:
        required_columns = REQUIRED_COLUMNS

    registered_fields = {
        field.name: field
        for field in registered_schema
    }

    incoming_fields = {
        field.name: field
        for field in incoming_schema
    }

    registered_names = set(registered_fields)
    incoming_names = set(incoming_fields)

    added_columns = []
    removed_columns = []
    changed_types = []
    nullability_changes = []
    breaking_reasons = []

    # ----------------------------------------------
    # A. Detect added columns
    # ----------------------------------------------

    for column_name in sorted(
        incoming_names - registered_names
    ):

        field = incoming_fields[column_name]

        added_columns.append({
            "column_name": column_name,
            "data_type": str(field.type),
            "nullable": field.nullable
        })

        if not field.nullable:

            breaking_reasons.append(
                f"New column '{column_name}' is not nullable"
            )

    # ----------------------------------------------
    # B. Detect removed columns
    # ----------------------------------------------

    for column_name in sorted(
        registered_names - incoming_names
    ):

        removed_columns.append(column_name)

        if column_name in required_columns:

            breaking_reasons.append(
                f"Required column '{column_name}' was removed"
            )

    # ----------------------------------------------
    # C. Detect data type changes
    # ----------------------------------------------

    common_columns = (
        registered_names & incoming_names
    )

    for column_name in sorted(common_columns):

        old_field = registered_fields[column_name]
        new_field = incoming_fields[column_name]

        if old_field.type != new_field.type:

            changed_types.append({
                "column_name": column_name,
                "registered_type": str(old_field.type),
                "incoming_type": str(new_field.type)
            })

            breaking_reasons.append(
                f"Data type changed for '{column_name}': "
                f"{old_field.type} -> {new_field.type}"
            )

        # A previously required column becoming nullable
        # weakens the existing data contract.
        if (
            not old_field.nullable
            and new_field.nullable
        ):

            nullability_changes.append({
                "column_name": column_name,
                "registered_nullable": old_field.nullable,
                "incoming_nullable": new_field.nullable
            })

            breaking_reasons.append(
                f"Column '{column_name}' changed "
                "from required to nullable"
            )

    # ----------------------------------------------
    # D. Check required columns
    # ----------------------------------------------

    missing_required = [
        column
        for column in required_columns
        if column not in incoming_fields
    ]

    # ----------------------------------------------
    # E. Determine classification
    # ----------------------------------------------

    has_changes = any([
        added_columns,
        removed_columns,
        changed_types,
        nullability_changes
    ])

    if breaking_reasons:

        classification = "BREAKING"
        decision = "QUARANTINE"

    elif has_changes:

        classification = "COMPATIBLE"
        decision = "ACCEPT"

    else:

        classification = "NO_CHANGE"
        decision = "ACCEPT"

    # ----------------------------------------------
    # F. Build comparison report
    # ----------------------------------------------

    report = {

        "classification": classification,

        "decision": decision,

        "added_columns": added_columns,

        "removed_columns": removed_columns,

        "changed_types": changed_types,

        "nullability_changes": nullability_changes,

        "missing_required_columns": missing_required,

        "breaking_reasons": breaking_reasons,

        "registered_fingerprint": schema_fingerprint(
            registered_schema
        ),

        "incoming_fingerprint": schema_fingerprint(
            incoming_schema
        )
    }

    return report


# --------------------------------------------------
# 6. COMPLETE SCHEMA DETECTION
# --------------------------------------------------

def detect_schema_drift(
    file_path,
    registered_schema=None,
    required_columns=None
):
    """
    Read an incoming Parquet file and detect schema drift.

    Returns a structured report.

    Malformed or inaccessible files receive an ERROR
    result instead of crashing the caller.
    """

    if registered_schema is None:
        registered_schema = REGISTERED_SCHEMA

    if required_columns is None:
        required_columns = REQUIRED_COLUMNS

    file_path = Path(file_path)

    try:

        incoming_schema = read_parquet_schema(
            file_path
        )

        comparison = compare_schemas(
            registered_schema,
            incoming_schema,
            required_columns
        )

        result = {

            "file_name": file_path.name,

            "file_path": str(file_path),

            "status": "SUCCESS",

            "registered_schema": schema_to_dict(
                registered_schema
            ),

            "incoming_schema": schema_to_dict(
                incoming_schema
            ),

            "comparison": comparison
        }

        return result

    except Exception as error:

        return {

            "file_name": file_path.name,

            "file_path": str(file_path),

            "status": "ERROR",

            "classification": "INVALID_INPUT",

            "decision": "QUARANTINE",

            "error": str(error)
        }


# --------------------------------------------------
# 7. PRINT SCHEMA DETECTION REPORT
# --------------------------------------------------

def print_detection_report(result):
    """
    Display a readable schema detection report.
    """

    print("\n" + "=" * 55)
    print("SCHEMAGUARD - SCHEMA DETECTION REPORT")
    print("=" * 55)

    print(f"File: {result['file_name']}")
    print(f"Status: {result['status']}")

    if result["status"] == "ERROR":

        print("Classification: INVALID_INPUT")
        print("Decision: QUARANTINE")
        print(f"Error: {result['error']}")

        print("=" * 55)

        return

    comparison = result["comparison"]

    print(
        f"Classification: {comparison['classification']}"
    )

    print(
        f"Decision: {comparison['decision']}"
    )

    print("\nAdded columns:")
    print(comparison["added_columns"])

    print("\nRemoved columns:")
    print(comparison["removed_columns"])

    print("\nChanged data types:")
    print(comparison["changed_types"])

    print("\nMissing required columns:")
    print(comparison["missing_required_columns"])

    print("\nBreaking reasons:")
    print(comparison["breaking_reasons"])

    print("\nSchema fingerprint:")
    print(comparison["incoming_fingerprint"])

    print("=" * 55)


# --------------------------------------------------
# 8. TEST ALL GENERATED BATCHES
# --------------------------------------------------

def test_generated_batches():
    """
    Run schema detection against generated test files.

    Each file is compared against the original
    registered baseline schema.
    """

    project_root = Path(__file__).resolve().parents[2]

    input_directory = (
        project_root / "data" / "input"
    )

    test_files = [
        "batch_01.parquet",
        "batch_02.parquet",
        "batch_03.parquet",
        "batch_04.parquet",
        "batch_05.parquet",
        "batch_06_empty.parquet",
        "batch_07_malformed.parquet"
    ]

    for file_name in test_files:

        file_path = input_directory / file_name

        result = detect_schema_drift(
            file_path,
            REGISTERED_SCHEMA,
            REQUIRED_COLUMNS
        )

        print_detection_report(result)


# --------------------------------------------------
# 9. DIRECT EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    test_generated_batches()
