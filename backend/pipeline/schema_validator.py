
"""
SchemaGuard - Schema and Data Quality Validator

Supports:
- Original required schema
- Compatible nullable columns: region, discount
- Record-level data quality validation
- Duplicate and foreign-key validation
- Rejection reason tracking
- Late-arriving record detection using event-time watermark
"""

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# --------------------------------------------------
# 1. BUSINESS RULES
# --------------------------------------------------

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "amount",
    "status"
]

OPTIONAL_COLUMNS = {
    "region": pa.string(),
    "discount": pa.float64()
}

VALID_STATUSES = {
    "PLACED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED"
}

REQUIRED_DATA_TYPES = {
    "order_id": pa.int64(),
    "customer_id": pa.int64(),
    "order_date": pa.timestamp("us", tz="UTC"),
    "amount": pa.float64(),
    "status": pa.string()
}


# --------------------------------------------------
# 2. VALIDATE SCHEMA STRUCTURE
# --------------------------------------------------

def validate_schema(schema):
    """
    Required columns must exist.
    Optional columns are accepted only with correct types.
    """

    errors = []

    incoming_fields = {
        field.name: field
        for field in schema
    }

    for column in REQUIRED_COLUMNS:
        if column not in incoming_fields:
            errors.append(
                f"Missing required column: {column}"
            )

    for column, expected_type in REQUIRED_DATA_TYPES.items():

        if column not in incoming_fields:
            continue

        actual_type = incoming_fields[column].type

        if actual_type != expected_type:
            errors.append(
                f"Invalid data type for {column}: "
                f"expected {expected_type}, got {actual_type}"
            )

    # Validate compatible optional columns
    for column, expected_type in OPTIONAL_COLUMNS.items():

        if column not in incoming_fields:
            continue

        actual_type = incoming_fields[column].type

        if actual_type != expected_type:
            errors.append(
                f"Invalid optional column type for {column}: "
                f"expected {expected_type}, got {actual_type}"
            )

        elif not incoming_fields[column].nullable:
            errors.append(
                f"Optional column {column} must be nullable"
            )

    # Reject unexpected columns
    allowed_columns = (
        set(REQUIRED_COLUMNS) |
        set(OPTIONAL_COLUMNS)
    )

    unexpected_columns = (
        set(incoming_fields) - allowed_columns
    )

    for column in sorted(unexpected_columns):
        errors.append(
            f"Unexpected column: {column}"
        )

    return {
        "is_valid": len(errors) == 0,
        "errors": errors
    }


# --------------------------------------------------
# 3. LOAD PARQUET DATA
# --------------------------------------------------

def load_parquet_data(file_path):

    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(
            f"File not found: {file_path}"
        )

    return pd.read_parquet(file_path)


# --------------------------------------------------
# 4. VALIDATE INDIVIDUAL RECORDS
# --------------------------------------------------

def validate_records(
    dataframe,
    valid_customer_ids,
    existing_order_ids=None,
    event_time_watermark=None,
    allowed_lateness_days=2
):
    """
    Validate business rules and identify late-arriving records.

    Late records are identified using an event-time watermark.
    Late records are NOT rejected only because they arrived late.
    """

    if existing_order_ids is None:
        existing_order_ids = set()

    df = dataframe.copy()

    valid_customer_ids = set(valid_customer_ids)
    existing_order_ids = set(existing_order_ids)

    rejection_reasons = [
        [] for _ in range(len(df))
    ]

    # A. Required field validation

    for column in REQUIRED_COLUMNS:

        missing_mask = df[column].isna()

        for position in range(len(df)):

            if missing_mask.iloc[position]:
                rejection_reasons[position].append(
                    f"Missing required value: {column}"
                )

    # B. Duplicate order ID validation

    seen_order_ids = set(existing_order_ids)

    for position, order_id in enumerate(df["order_id"]):

        if pd.isna(order_id):
            continue

        if order_id in seen_order_ids:

            rejection_reasons[position].append(
                f"Duplicate order_id: {order_id}"
            )

        else:
            seen_order_ids.add(order_id)

    # C. Amount validation

    for position, amount in enumerate(df["amount"]):

        if pd.isna(amount):
            continue

        try:
            if amount < 0:
                rejection_reasons[position].append(
                    "Amount cannot be negative"
                )

        except (TypeError, ValueError):
            rejection_reasons[position].append(
                "Amount must be numeric"
            )

    # D. Status validation

    for position, status in enumerate(df["status"]):

        if pd.isna(status):
            continue

        if status not in VALID_STATUSES:
            rejection_reasons[position].append(
                f"Invalid order status: {status}"
            )

    # E. Customer foreign-key validation

    for position, customer_id in enumerate(df["customer_id"]):

        if pd.isna(customer_id):
            continue

        if customer_id not in valid_customer_ids:
            rejection_reasons[position].append(
                f"Invalid customer reference: {customer_id}"
            )

    # F. Optional discount validation

    if "discount" in df.columns:

        for position, discount in enumerate(df["discount"]):

            if pd.isna(discount):
                continue

            try:
                if not 0 <= discount <= 100:
                    rejection_reasons[position].append(
                        f"Invalid discount value: {discount}. "
                        "Expected a percentage between 0 and 100"
                    )

            except (TypeError, ValueError):
                rejection_reasons[position].append(
                    "Discount must be numeric"
                )

    # G. Late-arrival detection

    df["is_late"] = False
    df["arrival_delay_days"] = pd.NA

    if event_time_watermark is not None:

        if allowed_lateness_days < 0:
            raise ValueError(
                "allowed_lateness_days cannot be negative"
            )

        watermark = pd.Timestamp(event_time_watermark)

        if watermark.tzinfo is None:
            watermark = watermark.tz_localize("UTC")
        else:
            watermark = watermark.tz_convert("UTC")

        event_dates = pd.to_datetime(
            df["order_date"],
            utc=True,
            errors="coerce"
        )

        lateness_limit = (
            watermark -
            pd.Timedelta(days=allowed_lateness_days)
        )

        late_mask = (
            event_dates.notna()
            & (event_dates < lateness_limit)
        )

        df["is_late"] = late_mask

        delay_days = (
            watermark - event_dates
        ).dt.total_seconds() / 86400

        df["arrival_delay_days"] = delay_days.where(
            late_mask
        )

    # H. Attach rejection reasons

    df["rejection_reasons"] = rejection_reasons

    valid_mask = df["rejection_reasons"].apply(
        lambda reasons: len(reasons) == 0
    )

    valid_records = df.loc[valid_mask].copy()

    rejected_records = df.loc[~valid_mask].copy()

    late_records = valid_records[
        valid_records["is_late"]
    ]

    # I. Summary

    summary = {
        "total_records": len(df),
        "valid_records": len(valid_records),
        "rejected_records": len(rejected_records),

        "late_records": len(late_records),

        "duplicate_records": sum(
            any(
                reason.startswith("Duplicate order_id")
                for reason in reasons
            )
            for reasons in rejection_reasons
        ),

        "missing_required_values": sum(
            any(
                reason.startswith("Missing required value")
                for reason in reasons
            )
            for reasons in rejection_reasons
        ),

        "invalid_amounts": sum(
            any(
                reason.startswith("Amount")
                for reason in reasons
            )
            for reasons in rejection_reasons
        ),

        "invalid_statuses": sum(
            any(
                reason.startswith("Invalid order status")
                for reason in reasons
            )
            for reasons in rejection_reasons
        ),

        "invalid_customer_references": sum(
            any(
                reason.startswith("Invalid customer reference")
                for reason in reasons
            )
            for reasons in rejection_reasons
        ),

        "invalid_discounts": sum(
            any(
                reason.startswith("Invalid discount")
                or reason.startswith("Discount")
                for reason in reasons
            )
            for reasons in rejection_reasons
        )
    }

    return (
        valid_records,
        rejected_records,
        summary
    )


# --------------------------------------------------
# 5. LOAD CUSTOMER REFERENCE DATA
# --------------------------------------------------

def load_customer_ids(customer_file):

    customers = pd.read_parquet(customer_file)

    if "customer_id" not in customers.columns:
        raise ValueError(
            "Customer reference file is missing customer_id"
        )

    return set(
        customers["customer_id"].dropna().tolist()
    )


# --------------------------------------------------
# 6. COMPLETE VALIDATION WORKFLOW
# --------------------------------------------------

def validate_parquet_file(
    file_path,
    customer_file,
    existing_order_ids=None,
    event_time_watermark=None,
    allowed_lateness_days=2
):

    file_path = Path(file_path)

    result = {
        "file_name": file_path.name,
        "status": "FAILED",
        "schema_validation": None,
        "summary": None,
        "valid_records": None,
        "rejected_records": None,
        "error": None
    }

    try:

        # Read schema
        schema = pq.read_schema(file_path)

        # Validate schema
        schema_result = validate_schema(schema)

        result["schema_validation"] = schema_result

        if not schema_result["is_valid"]:
            result["error"] = "Schema validation failed"
            return result

        # Load data
        dataframe = load_parquet_data(file_path)

        # Empty batch
        if dataframe.empty:

            result["status"] = "SUCCESS_EMPTY"

            result["summary"] = {
                "total_records": 0,
                "valid_records": 0,
                "rejected_records": 0,
                "late_records": 0
            }

            result["valid_records"] = dataframe
            result["rejected_records"] = dataframe.copy()

            return result

        # Load customer reference
        customer_ids = load_customer_ids(customer_file)

        # Validate records
        (
            valid_records,
            rejected_records,
            summary
        ) = validate_records(
            dataframe,
            customer_ids,
            existing_order_ids,
            event_time_watermark,
            allowed_lateness_days
        )

        result["status"] = "SUCCESS"
        result["summary"] = summary
        result["valid_records"] = valid_records
        result["rejected_records"] = rejected_records

        return result

    except Exception as error:

        result["error"] = str(error)

        return result


# --------------------------------------------------
# 7. PRINT VALIDATION REPORT
# --------------------------------------------------

def print_validation_report(result):

    print("\n" + "=" * 55)
    print("SCHEMAGUARD - DATA QUALITY REPORT")
    print("=" * 55)

    print(f"File: {result['file_name']}")
    print(f"Status: {result['status']}")

    if result["schema_validation"] is not None:

        print(
            "\nSchema valid:",
            result["schema_validation"]["is_valid"]
        )

        print(
            "Schema errors:",
            result["schema_validation"]["errors"]
        )

    if result["summary"] is not None:

        print("\nValidation summary:")

        for key, value in result["summary"].items():
            print(f"{key}: {value}")

    if result["error"]:
        print("\nError:", result["error"])

    print("=" * 55)


# --------------------------------------------------
# 8. SAVE REJECTED RECORDS
# --------------------------------------------------

def save_rejected_records(
    rejected_records,
    output_file
):

    output_file = Path(output_file)

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    if rejected_records is None or rejected_records.empty:
        return

    rejected_records.to_parquet(
        output_file,
        index=False
    )


# --------------------------------------------------
# 9. RUN VALIDATION TEST
# --------------------------------------------------

def test_batch_01():

    project_root = Path(__file__).resolve().parents[2]

    input_directory = project_root / "data" / "input"

    customer_file = input_directory / "customers.parquet"

    batch_file = input_directory / "batch_01.parquet"

    result = validate_parquet_file(
        batch_file,
        customer_file
    )

    print_validation_report(result)

    if result["status"] == "SUCCESS":

        output_file = (
            project_root /
            "data" /
            "quarantine" /
            "batch_01_rejected.parquet"
        )

        save_rejected_records(
            result["rejected_records"],
            output_file
        )

        print(
            "\nRejected records saved to:",
            output_file
        )


# --------------------------------------------------
# 10. DIRECT EXECUTION
# --------------------------------------------------

if __name__ == "__main__":
    test_batch_01()
