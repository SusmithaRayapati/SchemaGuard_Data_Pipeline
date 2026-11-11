
import pandas as pd
import pytest

from backend.pipeline.transformation import (
    REQUIRED_COLUMNS,
    TARGET_COLUMNS,
    validate_transformation_input,
    normalize_optional_columns,
    validate_discount_values,
    calculate_derived_columns,
    add_traceability_columns,
    build_target_dataframe,
    transform_records,
    save_transformed_data,
)


@pytest.fixture
def sample_data():
    return pd.DataFrame({
        "order_id": [101, 102],
        "customer_id": [1, 2],
        "order_date": [
            "2026-01-15",
            "2026-02-20"
        ],
        "amount": [1000.0, 2000.0],
        "status": ["PLACED", "DELIVERED"],
        "discount": [10.0, 20.0],
    })


# 1. Input validation

def test_valid_transformation_input(sample_data):
    assert validate_transformation_input(sample_data) is True


def test_missing_required_column(sample_data):
    df = sample_data.drop(columns=["order_id"])

    with pytest.raises(ValueError, match="Missing required columns"):
        validate_transformation_input(df)


def test_empty_dataframe_rejected(sample_data):
    df = sample_data.iloc[0:0]

    with pytest.raises(ValueError, match="empty DataFrame"):
        validate_transformation_input(df)


def test_null_order_id_rejected(sample_data):
    df = sample_data.copy()
    df.loc[0, "order_id"] = None

    with pytest.raises(ValueError, match="order_id cannot contain null"):
        validate_transformation_input(df)


def test_duplicate_order_id_rejected(sample_data):
    df = sample_data.copy()
    df.loc[1, "order_id"] = 101

    with pytest.raises(ValueError, match="Duplicate order_id"):
        validate_transformation_input(df)


# 2. Optional column normalization

def test_missing_optional_columns_are_added(sample_data):
    df = sample_data.drop(columns=["discount"])

    result = normalize_optional_columns(df)

    assert "region" in result.columns
    assert "discount" in result.columns
    assert result["region"].isna().all()
    assert (result["discount"] == 0.0).all()


def test_customer_id_converted_to_int64(sample_data):
    df = sample_data.copy()
    df["customer_id"] = df["customer_id"].astype(str)

    result = normalize_optional_columns(df)

    assert result["customer_id"].dtype == "int64"


def test_null_discount_becomes_zero(sample_data):
    df = sample_data.copy()
    df.loc[0, "discount"] = None

    result = normalize_optional_columns(df)

    assert result.loc[0, "discount"] == 0.0


# 3. Discount validation

def test_valid_discount_values(sample_data):
    assert validate_discount_values(sample_data) is True


@pytest.mark.parametrize("discount", [-5, 101])
def test_invalid_discount_values_rejected(sample_data, discount):
    df = sample_data.copy()
    df.loc[0, "discount"] = discount

    with pytest.raises(ValueError, match="Invalid discount"):
        validate_discount_values(df)


# 4. Derived calculations

def test_discount_and_net_amount_calculation(sample_data):
    result = calculate_derived_columns(sample_data)

    assert result.loc[0, "discount_amount"] == 100.0
    assert result.loc[0, "net_amount"] == 900.0

    assert result.loc[1, "discount_amount"] == 400.0
    assert result.loc[1, "net_amount"] == 1600.0


def test_negative_amount_rejected(sample_data):
    df = sample_data.copy()
    df.loc[0, "amount"] = -100

    with pytest.raises(ValueError, match="cannot be negative"):
        calculate_derived_columns(df)


def test_null_amount_rejected(sample_data):
    df = sample_data.copy()
    df.loc[0, "amount"] = None

    with pytest.raises(ValueError, match="Amount contains null"):
        calculate_derived_columns(df)


def test_invalid_amount_rejected(sample_data):
    df = sample_data.copy()
    df["amount"] = ["invalid", 2000]

    with pytest.raises((ValueError, TypeError)):
        calculate_derived_columns(df)


def test_order_date_converted_to_utc(sample_data):
    result = calculate_derived_columns(sample_data)

    assert str(result["order_date"].dt.tz) == "UTC"


def test_order_year_and_month_derived(sample_data):
    result = calculate_derived_columns(sample_data)

    assert result["order_year"].tolist() == [2026, 2026]
    assert result["order_month"].tolist() == [1, 2]


# 5. Traceability metadata

def test_traceability_columns_added(sample_data):
    result = add_traceability_columns(
        sample_data,
        "batch_01.parquet",
        "RUN_001"
    )

    assert (result["source_file"] == "batch_01.parquet").all()
    assert (result["run_id"] == "RUN_001").all()


@pytest.mark.parametrize(
    "source_file, run_id",
    [
        ("", "RUN_001"),
        ("batch_01.parquet", ""),
    ]
)
def test_empty_traceability_metadata_rejected(
    sample_data,
    source_file,
    run_id
):
    with pytest.raises(ValueError):
        add_traceability_columns(
            sample_data,
            source_file,
            run_id
        )


# 6. Target format

def test_target_dataframe_column_order(sample_data):
    df = sample_data.copy()

    df = normalize_optional_columns(df)
    df = calculate_derived_columns(df)
    df = add_traceability_columns(
        df,
        "batch_01.parquet",
        "RUN_001"
    )

    result = build_target_dataframe(df)

    assert result.columns.tolist() == TARGET_COLUMNS


def test_missing_target_column_rejected(sample_data):
    with pytest.raises(ValueError, match="Target columns are missing"):
        build_target_dataframe(sample_data)


# 7. Complete transformation

def test_complete_transformation(sample_data):
    result = transform_records(
        sample_data,
        "batch_01.parquet",
        "RUN_001"
    )

    assert len(result) == 2
    assert result.columns.tolist() == TARGET_COLUMNS
    assert result.loc[0, "net_amount"] == 900.0
    assert result.loc[0, "source_file"] == "batch_01.parquet"


def test_non_dataframe_input_rejected():
    with pytest.raises(TypeError, match="Pandas DataFrame"):
        transform_records(
            [{"order_id": 101}],
            "batch.parquet",
            "RUN_001"
        )


def test_rejection_reasons_removed(sample_data):
    df = sample_data.copy()
    df["rejection_reasons"] = ["", ""]

    result = transform_records(
        df,
        "batch_01.parquet",
        "RUN_001"
    )

    assert "rejection_reasons" not in result.columns


# 8. Parquet output

def test_save_transformed_data(sample_data, tmp_path):
    result = transform_records(
        sample_data,
        "batch_01.parquet",
        "RUN_001"
    )

    output_file = tmp_path / "output" / "transformed.parquet"

    saved_path = save_transformed_data(
        result,
        output_file
    )

    assert saved_path.exists()

    loaded_data = pd.read_parquet(saved_path)

    assert len(loaded_data) == 2
    assert loaded_data.columns.tolist() == TARGET_COLUMNS
