
import pytest
import pandas as pd
import pyarrow as pa

from backend.pipeline.schema_detector import REGISTERED_SCHEMA
from backend.pipeline.schema_validator import (
    validate_schema,
    validate_records,
    REQUIRED_COLUMNS
)


# Reusable valid test data
def create_test_dataframe(**overrides):

    data = {
        "order_id": [1001],
        "customer_id": [101],
        "order_date": [
            pd.Timestamp("2026-06-15", tz="UTC")
        ],
        "amount": [1500.0],
        "status": ["PLACED"],
        "discount": [10.0]
    }

    data.update(overrides)

    return pd.DataFrame(data)


# 1. Correct schema should be accepted
def test_valid_schema():

    result = validate_schema(REGISTERED_SCHEMA)

    assert result["is_valid"] is True
    assert result["errors"] == []


# 2. Missing required column should fail
def test_missing_required_column():

    incoming_schema = pa.schema([
        field
        for field in REGISTERED_SCHEMA
        if field.name != "status"
    ])

    result = validate_schema(incoming_schema)

    assert result["is_valid"] is False
    assert any(
        "Missing required column: status" in error
        for error in result["errors"]
    )


# 3. Incorrect data type should fail
def test_invalid_column_data_type():

    incoming_schema = pa.schema([
        pa.field("order_id", pa.string()),
        *list(REGISTERED_SCHEMA)[1:]
    ])

    result = validate_schema(incoming_schema)

    assert result["is_valid"] is False
    assert any(
        "Invalid data type for order_id" in error
        for error in result["errors"]
    )


# 4. Unexpected column should fail
def test_unexpected_column():

    incoming_schema = REGISTERED_SCHEMA.append(
        pa.field("unknown_column", pa.string())
    )

    result = validate_schema(incoming_schema)

    assert result["is_valid"] is False
    assert any(
        "Unexpected column: unknown_column" in error
        for error in result["errors"]
    )


# 5. Valid optional nullable column should pass
def test_valid_optional_column():

    incoming_schema = REGISTERED_SCHEMA.append(
        pa.field("region", pa.string(), nullable=True)
    )

    result = validate_schema(incoming_schema)

    assert result["is_valid"] is True


# 6. Non-nullable optional column should fail
def test_non_nullable_optional_column():

    incoming_schema = REGISTERED_SCHEMA.append(
        pa.field("region", pa.string(), nullable=False)
    )

    result = validate_schema(incoming_schema)

    assert result["is_valid"] is False
    assert any(
        "must be nullable" in error
        for error in result["errors"]
    )


# 7. Valid records should be accepted
def test_valid_records():

    df = create_test_dataframe()

    valid, rejected, summary = validate_records(
        df,
        valid_customer_ids={101}
    )

    assert len(valid) == 1
    assert len(rejected) == 0
    assert summary["valid_records"] == 1
    assert summary["rejected_records"] == 0


# 8. Duplicate order ID should be rejected
def test_duplicate_order_ids():

    df = create_test_dataframe(
        order_id=[1001, 1001],
        customer_id=[101, 101],
        order_date=[
            pd.Timestamp("2026-06-15", tz="UTC"),
            pd.Timestamp("2026-06-16", tz="UTC")
        ],
        amount=[1500.0, 2000.0],
        status=["PLACED", "SHIPPED"],
        discount=[10.0, 5.0]
    )

    valid, rejected, summary = validate_records(
        df,
        valid_customer_ids={101}
    )

    assert len(valid) == 1
    assert len(rejected) == 1
    assert summary["duplicate_records"] == 1


# 9. Negative amount should be rejected
def test_negative_amount():

    df = create_test_dataframe(amount=[-500.0])

    valid, rejected, summary = validate_records(
        df,
        valid_customer_ids={101}
    )

    assert len(valid) == 0
    assert len(rejected) == 1
    assert summary["invalid_amounts"] == 1


# 10. Invalid status should be rejected
def test_invalid_order_status():

    df = create_test_dataframe(status=["PENDING_UNKNOWN"])

    valid, rejected, summary = validate_records(
        df,
        valid_customer_ids={101}
    )

    assert len(valid) == 0
    assert summary["invalid_statuses"] == 1


# 11. Invalid customer reference should be rejected
def test_invalid_customer_reference():

    df = create_test_dataframe(customer_id=[999])

    valid, rejected, summary = validate_records(
        df,
        valid_customer_ids={101, 102}
    )

    assert len(valid) == 0
    assert summary["invalid_customer_references"] == 1


# 12. Invalid discount should be rejected
def test_invalid_discount():

    df = create_test_dataframe(discount=[150.0])

    valid, rejected, summary = validate_records(
        df,
        valid_customer_ids={101}
    )

    assert len(valid) == 0
    assert summary["invalid_discounts"] == 1


# 13. Existing order ID should be rejected
def test_existing_order_id():

    df = create_test_dataframe(order_id=[1001])

    valid, rejected, summary = validate_records(
        df,
        valid_customer_ids={101},
        existing_order_ids={1001}
    )

    assert len(valid) == 0
    assert summary["duplicate_records"] == 1


# 14. Late record should be detected but not rejected
def test_late_arriving_record():

    df = create_test_dataframe(
        order_date=[
            pd.Timestamp("2026-06-15", tz="UTC")
        ]
    )

    valid, rejected, summary = validate_records(
        df,
        valid_customer_ids={101},
        event_time_watermark="2026-07-01T00:00:00Z",
        allowed_lateness_days=2
    )

    assert len(valid) == 1
    assert len(rejected) == 0
    assert summary["late_records"] == 1
    assert bool(valid.iloc[0]["is_late"]) is True


# 15. Negative allowed lateness should raise an error
def test_negative_allowed_lateness():

    df = create_test_dataframe()

    with pytest.raises(ValueError):
        validate_records(
            df,
            valid_customer_ids={101},
            event_time_watermark="2026-07-01T00:00:00Z",
            allowed_lateness_days=-1
        )
