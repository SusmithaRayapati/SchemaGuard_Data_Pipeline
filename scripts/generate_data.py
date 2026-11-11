
"""
SchemaGuard - Synthetic Parquet Data Generator

Generates reproducible e-commerce order batches for testing:
1. Original schema
2. Compatible nullable column addition
3. Additional compatible column
4. Incompatible primary-key type change
5. Missing required column
6. Empty Parquet batch
7. Malformed Parquet input

Data quality scenarios:
- Duplicate order IDs
- Missing required values
- Negative amounts
- Invalid statuses
- Invalid customer references
- Boundary values
- Late-arriving records
- Out-of-order records
"""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

INPUT_DIR = PROJECT_ROOT / "data" / "input"
EXPECTED_DIR = PROJECT_ROOT / "data" / "expected"

SEED = 42

VALID_STATUSES = [
    "PLACED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED"
]

REGIONS = [
    "Hyderabad",
    "Chennai",
    "Bengaluru",
    "Mumbai"
]

CUSTOMER_SEGMENTS = [
    "Retail",
    "Premium",
    "Wholesale"
]


# --------------------------------------------------
# 2. DEFINE SOURCE SCHEMAS
# --------------------------------------------------

BASE_SCHEMA = pa.schema([
    pa.field("order_id", pa.int64()),
    pa.field("customer_id", pa.int64()),
    pa.field("order_date", pa.timestamp("us", tz="UTC")),
    pa.field("amount", pa.float64()),
    pa.field("status", pa.string())
])

SCHEMA_V2 = BASE_SCHEMA.append(
    pa.field("region", pa.string())
)

SCHEMA_V3 = SCHEMA_V2.append(
    pa.field("discount", pa.float64())
)

# Breaking schema: order_id changes from int64 to string.
SCHEMA_V4 = pa.schema([
    pa.field("order_id", pa.string()),
    pa.field("customer_id", pa.int64()),
    pa.field("order_date", pa.timestamp("us", tz="UTC")),
    pa.field("amount", pa.float64()),
    pa.field("status", pa.string()),
    pa.field("region", pa.string()),
    pa.field("discount", pa.float64())
])

# Breaking schema: required status column is removed.
SCHEMA_V5 = pa.schema([
    pa.field("order_id", pa.int64()),
    pa.field("customer_id", pa.int64()),
    pa.field("order_date", pa.timestamp("us", tz="UTC")),
    pa.field("amount", pa.float64()),
    pa.field("region", pa.string()),
    pa.field("discount", pa.float64())
])


# --------------------------------------------------
# 3. CREATE DIRECTORY STRUCTURE
# --------------------------------------------------

def create_directories():
    """Create required data directories."""

    INPUT_DIR.mkdir(parents=True, exist_ok=True)
    EXPECTED_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# 4. GENERATE CUSTOMER REFERENCE DATA
# --------------------------------------------------

def generate_customers(rng):
    """
    Generate customer master data.

    customer_id is the primary key.
    Orders reference these customer IDs.
    """

    customers = []

    for customer_id in range(1001, 1101):

        customers.append({
            "customer_id": customer_id,
            "customer_name": f"Customer_{customer_id}",
            "city": rng.choice(REGIONS),
            "customer_segment": rng.choice(CUSTOMER_SEGMENTS)
        })

    customer_schema = pa.schema([
        pa.field("customer_id", pa.int64()),
        pa.field("customer_name", pa.string()),
        pa.field("city", pa.string()),
        pa.field("customer_segment", pa.string())
    ])

    table = pa.Table.from_pylist(
        customers,
        schema=customer_schema
    )

    pq.write_table(
        table,
        INPUT_DIR / "customers.parquet",
        compression="snappy"
    )

    return customers


# --------------------------------------------------
# 5. GENERATE NORMAL ORDER RECORDS
# --------------------------------------------------

def generate_order_rows(batch_number, row_count, rng):
    """
    Generate deterministic order records.

    Each batch gets a separate chronological
    91-day event-time window.
    """

    rows = []

    start_order_id = 100000 + (
        (batch_number - 1) * row_count
    )

    # Each batch starts after the previous batch's
    # 91-day date window.
    start_date = datetime(
        2026, 1, 1, tzinfo=timezone.utc
    ) + timedelta(
        days=(batch_number - 1) * 91
    )

    for i in range(row_count):

        order_date = start_date + timedelta(
            days=rng.randint(0, 90),
            seconds=rng.randint(0, 86399)
        )

        row = {
            "order_id": start_order_id + i,
            "customer_id": rng.randint(1001, 1100),
            "order_date": order_date,
            "amount": round(
                rng.uniform(100, 50000), 2
            ),
            "status": rng.choice(VALID_STATUSES)
        }

        if batch_number >= 2:
            row["region"] = rng.choice(REGIONS)

        if batch_number >= 3:
            row["discount"] = round(
                rng.uniform(0, 30), 2
            )

        rows.append(row)

    # Boundary-value scenarios
    rows[0]["amount"] = 0.0
    rows[1]["amount"] = 50000.0

    return rows


# --------------------------------------------------
# 6. INJECT DATA QUALITY ISSUES
# --------------------------------------------------

def inject_data_quality_issues(rows):
    """
    Add deliberately invalid and duplicate records.

    These records have valid Parquet structure.
    They are intended to be rejected by data-quality
    validation, not schema validation.
    """

    # Missing required customer ID
    rows.append({
        **rows[0],
        "order_id": 900001,
        "customer_id": None
    })

    # Negative amount
    rows.append({
        **rows[1],
        "order_id": 900002,
        "amount": -500.00
    })

    # Invalid order status
    rows.append({
        **rows[2],
        "order_id": 900003,
        "status": "UNKNOWN"
    })

    # Invalid foreign-key reference
    rows.append({
        **rows[3],
        "order_id": 900004,
        "customer_id": 999999
    })

    # Missing required amount
    rows.append({
        **rows[4],
        "order_id": 900006,
        "amount": None
    })

    # Exact duplicate order record
    rows.append(rows[4].copy())


# --------------------------------------------------
# 7. ADD LATE-ARRIVING RECORD
# --------------------------------------------------

def add_late_arriving_record(rows, batch_number):
    """
    Add one deliberately late event to Batch 3.

    Normal Batch 3 records belong to July-September 2026.
    This record belongs to January 2025.
    """

    if batch_number != 3:
        return

    late_record = {
        "order_id": 900010,
        "customer_id": 1001,
        "order_date": datetime(
            2025, 1, 1, tzinfo=timezone.utc
        ),
        "amount": 1500.00,
        "status": "DELIVERED",
        "region": "Hyderabad",
        "discount": 10.0
    }

    rows.append(late_record)


# --------------------------------------------------
# 8. WRITE PARQUET FILE WITH EXPLICIT SCHEMA
# --------------------------------------------------

def write_parquet(rows, file_path, schema):
    """
    Write Parquet using an explicitly defined Arrow schema.

    This prevents accidental type inference from changing
    our intended data types.
    """

    table = pa.Table.from_pylist(
        rows,
        schema=schema
    )

    pq.write_table(
        table,
        file_path,
        compression="snappy"
    )

    return table.num_rows


# --------------------------------------------------
# 9. GENERATE EMPTY BATCH
# --------------------------------------------------

def generate_empty_batch():

    empty_table = pa.Table.from_pylist(
        [],
        schema=BASE_SCHEMA
    )

    pq.write_table(
        empty_table,
        INPUT_DIR / "batch_06_empty.parquet",
        compression="snappy"
    )


# --------------------------------------------------
# 10. GENERATE MALFORMED INPUT
# --------------------------------------------------

def generate_malformed_file():
    """
    Create a deliberately invalid Parquet file.
    """

    malformed_path = (
        INPUT_DIR / "batch_07_malformed.parquet"
    )

    malformed_path.write_bytes(
        b"This is not a valid Parquet file."
    )


# --------------------------------------------------
# 11. GENERATE EXPECTED RESULTS MANIFEST
# --------------------------------------------------

def generate_expected_manifest(
    rows_per_batch,
    seed,
    duplicate_order_id
):
    """
    Keep expected-answer labels separate from source data.
    """

    manifest = {

        "project": "SchemaGuard",

        "dataset": "Ecommerce Orders",

        "seed": seed,

        "rows_per_batch": rows_per_batch,

        "batches": [

            {
                "file": "batch_01.parquet",
                "expected_decision": "ACCEPT",
                "scenario": "Original schema with data-quality issues"
            },

            {
                "file": "batch_02.parquet",
                "expected_decision": "ACCEPT",
                "scenario": "Adds nullable region column"
            },

            {
                "file": "batch_03.parquet",
                "expected_decision": "ACCEPT",
                "scenario": "Adds nullable discount column and late record"
            },

            {
                "file": "batch_04.parquet",
                "expected_decision": "QUARANTINE",
                "scenario": "Primary key changes from int64 to string"
            },

            {
                "file": "batch_05.parquet",
                "expected_decision": "QUARANTINE",
                "scenario": "Required status column is missing"
            },

            {
                "file": "batch_06_empty.parquet",
                "expected_decision": "ACCEPT_EMPTY",
                "scenario": "Valid schema with zero records"
            },

            {
                "file": "batch_07_malformed.parquet",
                "expected_decision": "QUARANTINE",
                "scenario": "Malformed Parquet input"
            }
        ],

        "data_quality_expectations": {

            "missing_customer_id": 900001,

            "negative_amount": 900002,

            "invalid_status": 900003,

            "invalid_customer_reference": 900004,

            "missing_amount": 900006,

            "duplicate_order_id": duplicate_order_id,

            "late_arriving_order_id": 900010,

            "boundary_amounts": [
                0.0,
                50000.0
            ]
        }
    }

    manifest_path = (
        EXPECTED_DIR / "expected_manifest.json"
    )

    with open(
        manifest_path,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            manifest,
            file,
            indent=4
        )

    print(
        f"Expected manifest created: {manifest_path}"
    )


# --------------------------------------------------
# 12. MAIN DATA GENERATION FUNCTION
# --------------------------------------------------

def create_batches(rows_per_batch=100, seed=42):
    """
    Generate five schema-version batches,
    one empty batch and one malformed file.
    """

    if rows_per_batch < 5:
        raise ValueError(
            "rows_per_batch must be at least 5"
        )

    rng = random.Random(seed)

    create_directories()

    print("\n======================================")
    print("     SCHEMAGUARD DATA GENERATOR")
    print("======================================\n")

    # Generate customer reference data
    generate_customers(rng)

    print("Generated customers.parquet")

    schema_mapping = {
        1: BASE_SCHEMA,
        2: SCHEMA_V2,
        3: SCHEMA_V3,
        4: SCHEMA_V4,
        5: SCHEMA_V5
    }

    duplicate_order_id = None

    # Generate five schema-version batches
    for batch_number in range(1, 6):

        rows = generate_order_rows(
            batch_number,
            rows_per_batch,
            rng
        )

        if batch_number == 1:

            inject_data_quality_issues(rows)

            duplicate_order_id = rows[-1]["order_id"]

        if batch_number == 3:

            add_late_arriving_record(
                rows,
                batch_number
            )

        # Introduce out-of-order records
        rng.shuffle(rows)

        # Convert primary key to string in Batch 4
        if batch_number == 4:

            for row in rows:
                row["order_id"] = str(row["order_id"])

        file_name = f"batch_{batch_number:02d}.parquet"

        file_path = INPUT_DIR / file_name

        schema = schema_mapping[batch_number]

        record_count = write_parquet(
            rows,
            file_path,
            schema
        )

        print(
            f"Generated {file_name} | "
            f"Rows: {record_count} | "
            f"Columns: {len(schema)}"
        )

    # Empty batch
    generate_empty_batch()

    print("Generated batch_06_empty.parquet | Rows: 0")

    # Malformed batch
    generate_malformed_file()

    print("Generated batch_07_malformed.parquet")

    # Expected results
    generate_expected_manifest(
        rows_per_batch,
        seed,
        duplicate_order_id
    )

    print("\n======================================")
    print("DATA GENERATION COMPLETED")
    print("======================================")

    print(f"\nInput directory: {INPUT_DIR}")
    print(f"Expected directory: {EXPECTED_DIR}")

    print("\nGenerated scenarios:")
    print("1. Original schema")
    print("2. Compatible nullable column")
    print("3. Second compatible schema change")
    print("4. Incompatible primary-key type")
    print("5. Missing required column")
    print("6. Empty Parquet batch")
    print("7. Malformed Parquet file")
    print("8. Duplicate records")
    print("9. Missing required values")
    print("10. Invalid reference keys")
    print("11. Negative amounts")
    print("12. Late-arriving records")
    print("13. Boundary values")


# --------------------------------------------------
# 13. COMMAND-LINE EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=(
            "SchemaGuard - Reproducible "
            "Parquet Schema Evolution Data Generator"
        )
    )

    parser.add_argument(
        "--rows-per-batch",
        type=int,
        default=100,
        help="Number of normal records in each regular batch"
    )

    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducibility"
    )

    args = parser.parse_args()

    create_batches(
        rows_per_batch=args.rows_per_batch,
        seed=args.seed
    )
