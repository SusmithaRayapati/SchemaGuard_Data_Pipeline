
import pytest
import pyarrow as pa
import pyarrow.parquet as pq

from backend.pipeline.schema_detector import (
    REGISTERED_SCHEMA,
    REQUIRED_COLUMNS,
    schema_fingerprint,
    read_parquet_schema,
    schema_to_dict,
    compare_schemas,
    detect_schema_drift
)


# 1. Identical schemas should have identical fingerprints
def test_schema_fingerprint_is_deterministic():

    fingerprint1 = schema_fingerprint(REGISTERED_SCHEMA)
    fingerprint2 = schema_fingerprint(REGISTERED_SCHEMA)

    assert fingerprint1 == fingerprint2
    assert len(fingerprint1) == 64


# 2. Schema dictionary conversion
def test_schema_to_dict():

    result = schema_to_dict(REGISTERED_SCHEMA)

    assert "columns" in result
    assert result["column_count"] == 5
    assert "fingerprint" in result


# 3. Identical schemas should return NO_CHANGE
def test_no_schema_change():

    result = compare_schemas(
        REGISTERED_SCHEMA,
        REGISTERED_SCHEMA,
        REQUIRED_COLUMNS
    )

    assert result["classification"] == "NO_CHANGE"
    assert result["decision"] == "ACCEPT"


# 4. Adding a nullable column should be COMPATIBLE
def test_nullable_column_addition():

    incoming_schema = REGISTERED_SCHEMA.append(
        pa.field("region", pa.string(), nullable=True)
    )

    result = compare_schemas(
        REGISTERED_SCHEMA,
        incoming_schema
    )

    assert result["classification"] == "COMPATIBLE"
    assert result["decision"] == "ACCEPT"
    assert result["added_columns"][0]["column_name"] == "region"


# 5. Adding a non-nullable column should be BREAKING
def test_non_nullable_column_addition():

    incoming_schema = REGISTERED_SCHEMA.append(
        pa.field("region", pa.string(), nullable=False)
    )

    result = compare_schemas(
        REGISTERED_SCHEMA,
        incoming_schema
    )

    assert result["classification"] == "BREAKING"
    assert result["decision"] == "QUARANTINE"


# 6. Removing a required column should be BREAKING
def test_required_column_removal():

    incoming_schema = pa.schema([
        field
        for field in REGISTERED_SCHEMA
        if field.name != "status"
    ])

    result = compare_schemas(
        REGISTERED_SCHEMA,
        incoming_schema
    )

    assert result["classification"] == "BREAKING"
    assert "status" in result["missing_required_columns"]


# 7. Changing a column data type should be BREAKING
def test_column_data_type_change():

    incoming_schema = pa.schema([
        pa.field("order_id", pa.string()),
        *list(REGISTERED_SCHEMA)[1:]
    ])

    result = compare_schemas(
        REGISTERED_SCHEMA,
        incoming_schema
    )

    assert result["classification"] == "BREAKING"
    assert result["decision"] == "QUARANTINE"
    assert len(result["changed_types"]) == 1


# 8. Missing Parquet file should raise FileNotFoundError
def test_missing_parquet_file(tmp_path):

    file_path = tmp_path / "missing.parquet"

    with pytest.raises(FileNotFoundError):
        read_parquet_schema(file_path)


# 9. Malformed Parquet input should return ERROR
def test_malformed_parquet_file(tmp_path):

    file_path = tmp_path / "invalid.parquet"
    file_path.write_text("This is not a valid Parquet file")

    result = detect_schema_drift(file_path)

    assert result["status"] == "ERROR"
    assert result["classification"] == "INVALID_INPUT"
    assert result["decision"] == "QUARANTINE"
