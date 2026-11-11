
import sqlite3

import pandas as pd
import pytest

from backend.services import pipeline_service as service


@pytest.fixture
def pipeline_setup(tmp_path, monkeypatch):
    """Create an isolated pipeline environment for integration tests."""

    input_dir = tmp_path / "data" / "input"
    processed_dir = tmp_path / "data" / "processed"
    data_dir = tmp_path / "data"

    input_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)

    batch_file = input_dir / "batch_test.parquet"
    batch_file.touch()

    customer_file = input_dir / "customers.parquet"
    customer_file.touch()

    database_path = data_dir / "schemaguard.db"

    with sqlite3.connect(database_path) as connection:
        connection.execute("""
            CREATE TABLE batch_metadata (
                run_id TEXT,
                batch_status TEXT,
                processed_file TEXT,
                source_file TEXT,
                created_at TEXT
            )
        """)

    monkeypatch.setattr(service, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(service, "INPUT_DIR", input_dir)
    monkeypatch.setattr(service, "CUSTOMER_FILE", customer_file)
    monkeypatch.setattr(service, "PROCESSED_DIR", processed_dir)

    monkeypatch.setattr(
        service, "initialize_database", lambda: None
    )

    return {
        "input_dir": input_dir,
        "processed_dir": processed_dir,
        "database_path": database_path,
        "batch_file": batch_file,
        "customer_file": customer_file,
    }


def configure_successful_pipeline(monkeypatch):
    """Mock external stages while testing orchestration."""

    valid_records = pd.DataFrame({
        "order_id": [1001],
        "customer_id": [101],
        "amount": [1500.0],
        "status": ["PLACED"]
    })

    rejected_records = pd.DataFrame()

    summary = {
        "total_records": 1,
        "valid_records": 1,
        "rejected_records": 0,
        "late_records": 0
    }

    monkeypatch.setattr(
        service,
        "inspect_batch_schema",
        lambda path: {
            "status": "NO_CHANGE",
            "schema": None,
            "fingerprint": "test-fingerprint",
            "details": {}
        }
    )

    monkeypatch.setattr(
        service,
        "get_previous_event_time_watermark",
        lambda: None
    )

    monkeypatch.setattr(
        service,
        "validate_parquet_file",
        lambda *args, **kwargs: {
            "status": "SUCCESS",
            "valid_records": valid_records,
            "rejected_records": rejected_records,
            "summary": summary
        }
    )

    monkeypatch.setattr(
        service,
        "transform_records",
        lambda dataframe, **kwargs: dataframe.copy()
    )

    monkeypatch.setattr(
        service,
        "save_transformed_data",
        lambda dataframe, output_file: None
    )

    monkeypatch.setattr(
        service, "start_pipeline_run", lambda **kwargs: None
    )

    monkeypatch.setattr(
        service, "record_batch", lambda **kwargs: None
    )

    monkeypatch.setattr(
        service, "record_quarantine", lambda **kwargs: None
    )

    monkeypatch.setattr(
        service, "record_metric", lambda **kwargs: None
    )

    monkeypatch.setattr(
        service, "complete_pipeline_run", lambda **kwargs: None
    )


# 1. Successful pipeline execution
def test_pipeline_success(pipeline_setup, monkeypatch):

    configure_successful_pipeline(monkeypatch)

    result = service.run_pipeline("batch_test.parquet")

    assert result["status"] == "SUCCESS"
    assert result["schema_status"] == "NO_CHANGE"
    assert result["input_records"] == 1
    assert result["output_records"] == 1
    assert result["rejected_records"] == 0
    assert result["late_records"] == 0


# 2. Breaking schema should quarantine the complete batch
def test_breaking_schema_quarantine(pipeline_setup, monkeypatch):

    monkeypatch.setattr(
        service,
        "inspect_batch_schema",
        lambda path: {
            "status": "BREAKING",
            "schema": None,
            "fingerprint": "breaking-fingerprint",
            "details": {
                "changed_columns": {"order_id": "string"}
            }
        }
    )

    monkeypatch.setattr(
        service, "start_pipeline_run", lambda **kwargs: None
    )

    recorded = {}

    monkeypatch.setattr(
        service,
        "quarantine_batch",
        lambda **kwargs: {
            "quarantine_id": "Q_TEST_001",
            "manifest_file": "test_manifest.json",
            "manifest": {"record_count": 0}
        }
    )

    monkeypatch.setattr(
        service,
        "record_batch",
        lambda **kwargs: recorded.update(kwargs)
    )

    monkeypatch.setattr(
        service,
        "record_quarantine",
        lambda **kwargs: recorded.update(kwargs)
    )

    monkeypatch.setattr(
        service,
        "complete_pipeline_run",
        lambda **kwargs: recorded.update(kwargs)
    )

    result = service.run_pipeline("batch_test.parquet")

    assert result["status"] == "PARTIAL"
    assert result["schema_status"] == "BREAKING"
    assert recorded["batch_status"] == "QUARANTINED"
    assert recorded["quarantine_reason"] == "BREAKING_SCHEMA_CHANGE"


# 3. Previously processed batch should be skipped
def test_pipeline_idempotency(pipeline_setup):

    database_path = pipeline_setup["database_path"]

    with sqlite3.connect(database_path) as connection:
        connection.execute(
            """
            INSERT INTO batch_metadata
            (run_id, batch_status, processed_file, source_file, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                "SG_PREVIOUS",
                "SUCCESS",
                "previous_output.parquet",
                "batch_test.parquet",
                "2026-11-11"
            )
        )

    result = service.run_pipeline("batch_test.parquet")

    assert result["status"] == "SKIPPED_ALREADY_PROCESSED"
    assert result["previous_run_id"] == "SG_PREVIOUS"
    assert result["previous_status"] == "SUCCESS"


# 4. Path traversal should be rejected
def test_pipeline_rejects_invalid_path(pipeline_setup):

    with pytest.raises(ValueError, match="Only an input filename"):
        service.run_pipeline("../outside.parquet")


# 5. Missing input file should raise FileNotFoundError
def test_pipeline_missing_input_file(pipeline_setup):

    with pytest.raises(FileNotFoundError, match="Input batch not found"):
        service.run_pipeline("missing_batch.parquet")
