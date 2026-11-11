
"""
SchemaGuard - Data Transformation Engine

Responsibilities:
1. Normalize approved schema versions.
2. Handle optional columns.
3. Calculate discount amount and net amount.
4. Derive order year and month.
5. Add source-file and run-ID metadata.
6. Produce a consistent target DataFrame.

Only validated records should enter this module.
"""

from pathlib import Path

import pandas as pd


# --------------------------------------------------
# 1. PROJECT CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"

REQUIRED_COLUMNS = [
    "order_id",
    "customer_id",
    "order_date",
    "amount",
    "status"
]

OPTIONAL_COLUMNS = [
    "region",
    "discount"
]

TARGET_COLUMNS = [
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


# --------------------------------------------------
# 2. VALIDATE TRANSFORMATION INPUT
# --------------------------------------------------

def validate_transformation_input(dataframe):
    """
    Check whether the DataFrame contains the required
    columns before transformation.
    """

    missing_columns = [
        column
        for column in REQUIRED_COLUMNS
        if column not in dataframe.columns
    ]

    if missing_columns:
        raise ValueError(
            f"Missing required columns: {missing_columns}"
        )

    if dataframe.empty:
        raise ValueError(
            "Cannot transform an empty DataFrame"
        )

    if dataframe["order_id"].isna().any():
        raise ValueError(
            "order_id cannot contain null values"
        )

    if dataframe["order_id"].duplicated().any():
        raise ValueError(
            "Duplicate order_id values found"
        )

    return True


# --------------------------------------------------
# 3. NORMALIZE OPTIONAL COLUMNS
# --------------------------------------------------

def normalize_optional_columns(dataframe):
    """
    Add optional columns when they are absent.

    Business assumptions:
    - Missing region remains NULL.
    - Missing discount is treated as 0%.
    - customer_id is stored as int64.
    """

    df = dataframe.copy()

    # Add region if missing.
    if "region" not in df.columns:
        df["region"] = pd.NA

    # Add discount if missing.
    if "discount" not in df.columns:
        df["discount"] = 0.0

    # Standardize customer_id datatype.
    df["customer_id"] = pd.to_numeric(
        df["customer_id"],
        errors="raise"
    ).astype("int64")

    # Null discount means no discount.
    df["discount"] = (
        pd.to_numeric(
            df["discount"],
            errors="raise"
        )
        .fillna(0.0)
        .astype("float64")
    )

    return df


# --------------------------------------------------
# 4. VALIDATE DISCOUNT VALUES
# --------------------------------------------------

def validate_discount_values(dataframe):
    """
    Discount is defined as a percentage.

    Allowed range: 0 to 100.
    """

    invalid_discount = (
        (dataframe["discount"] < 0)
        |
        (dataframe["discount"] > 100)
    )

    if invalid_discount.any():

        invalid_values = (
            dataframe.loc[
                invalid_discount,
                "discount"
            ].tolist()
        )

        raise ValueError(
            f"Invalid discount percentages: {invalid_values[:10]}"
        )

    return True


# --------------------------------------------------
# 5. CALCULATE DERIVED BUSINESS COLUMNS
# --------------------------------------------------

def calculate_derived_columns(dataframe):
    """
    Calculate:
    - discount_amount
    - net_amount
    - order_year
    - order_month
    """

    df = dataframe.copy()

    # Convert amount to numeric.
    df["amount"] = pd.to_numeric(
        df["amount"],
        errors="raise"
    )

    if df["amount"].isna().any():
        raise ValueError(
            "Amount contains null values"
        )

    if (df["amount"] < 0).any():
        raise ValueError(
            "Amount cannot be negative"
        )

    # Discount amount = amount * discount percentage.
    df["discount_amount"] = (
        df["amount"] * df["discount"] / 100
    ).round(2)

    # Net amount = amount - discount amount.
    df["net_amount"] = (
        df["amount"] - df["discount_amount"]
    ).round(2)

    # Convert order date to UTC.
    df["order_date"] = pd.to_datetime(
        df["order_date"],
        utc=True,
        errors="raise"
    )

    # Date-based analytical columns.
    df["order_year"] = (
        df["order_date"].dt.year.astype("int64")
    )

    df["order_month"] = (
        df["order_date"].dt.month.astype("int64")
    )

    return df


# --------------------------------------------------
# 6. ADD TRACEABILITY METADATA
# --------------------------------------------------

def add_traceability_columns(
    dataframe,
    source_file,
    run_id
):
    """
    Add metadata to identify where each record came from.
    """

    if not source_file:
        raise ValueError(
            "source_file cannot be empty"
        )

    if not run_id:
        raise ValueError(
            "run_id cannot be empty"
        )

    df = dataframe.copy()

    df["source_file"] = str(source_file)

    df["run_id"] = str(run_id)

    return df


# --------------------------------------------------
# 7. BUILD STANDARD TARGET FORMAT
# --------------------------------------------------

def build_target_dataframe(dataframe):
    """
    Arrange columns in a consistent target order.
    """

    missing_target_columns = [
        column
        for column in TARGET_COLUMNS
        if column not in dataframe.columns
    ]

    if missing_target_columns:
        raise ValueError(
            "Target columns are missing: "
            f"{missing_target_columns}"
        )

    return dataframe[TARGET_COLUMNS].copy()


# --------------------------------------------------
# 8. COMPLETE TRANSFORMATION FUNCTION
# --------------------------------------------------

def transform_records(
    dataframe,
    source_file,
    run_id
):
    """
    Execute the complete transformation process.

    Input:
        Validated source DataFrame.

    Output:
        Standardized target DataFrame.
    """

    if not isinstance(dataframe, pd.DataFrame):
        raise TypeError(
            "Input must be a Pandas DataFrame"
        )

    # Step 1: Validate input.
    validate_transformation_input(dataframe)

    # Step 2: Remove validator-only metadata.
    df = dataframe.drop(
        columns=["rejection_reasons"],
        errors="ignore"
    ).copy()

    # Step 3: Normalize optional columns and datatypes.
    df = normalize_optional_columns(df)

    # Step 4: Validate discount percentage.
    validate_discount_values(df)

    # Step 5: Calculate derived columns.
    df = calculate_derived_columns(df)

    # Step 6: Add traceability metadata.
    df = add_traceability_columns(
        df,
        source_file,
        run_id
    )

    # Step 7: Standardize target columns.
    target_df = build_target_dataframe(df)

    return target_df


# --------------------------------------------------
# 9. SAVE TRANSFORMED DATA
# --------------------------------------------------

def save_transformed_data(
    dataframe,
    output_file
):
    """
    Save transformed records as Parquet.
    """

    output_file = Path(output_file)

    output_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    dataframe.to_parquet(
        output_file,
        index=False,
        compression="snappy"
    )

    return output_file


# --------------------------------------------------
# 10. DISPLAY TRANSFORMATION REPORT
# --------------------------------------------------

def print_transformation_report(
    source_dataframe,
    target_dataframe,
    source_file,
    run_id
):

    print("\n" + "=" * 55)

    print("SCHEMAGUARD - TRANSFORMATION REPORT")

    print("=" * 55)

    print(f"Source file: {source_file}")

    print(f"Run ID: {run_id}")

    print(
        f"Input records: {len(source_dataframe)}"
    )

    print(
        f"Output records: {len(target_dataframe)}"
    )

    print(
        f"Output columns: {len(target_dataframe.columns)}"
    )

    print("\nTarget columns:")

    for column in target_dataframe.columns:
        print(
            f"- {column}: "
            f"{target_dataframe[column].dtype}"
        )

    print("\nSample transformed records:")

    print(
        target_dataframe.head(5).to_string(
            index=False
        )
    )

    print("\nNet amount calculation:")

    print(
        "net_amount = amount - discount_amount"
    )

    print("=" * 55)


# --------------------------------------------------
# 11. TEST TRANSFORMATION USING BATCH 01
# --------------------------------------------------

def test_batch_01_transformation():

    from backend.pipeline.schema_validator import (
        validate_parquet_file
    )

    input_directory = PROJECT_ROOT / "data" / "input"

    batch_file = input_directory / "batch_01.parquet"

    customer_file = input_directory / "customers.parquet"

    # First validate the source batch.
    validation_result = validate_parquet_file(
        batch_file,
        customer_file
    )

    if validation_result["status"] != "SUCCESS":
        raise RuntimeError(
            "Batch validation failed: "
            f"{validation_result['error']}"
        )

    # Only valid records enter transformation.
    valid_records = validation_result["valid_records"]

    run_id = "SCHEMAGUARD_TEST_001"

    transformed_data = transform_records(
        valid_records,
        source_file=batch_file.name,
        run_id=run_id
    )

    # Save preview output.
    output_file = (
        PROCESSED_DIR / "batch_01_transformed.parquet"
    )

    save_transformed_data(
        transformed_data,
        output_file
    )

    # Display results.
    print_transformation_report(
        valid_records,
        transformed_data,
        batch_file.name,
        run_id
    )

    print("\nTransformed file saved to:")

    print(output_file)


# --------------------------------------------------
# 12. DIRECT EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    test_batch_01_transformation()
