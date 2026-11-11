# SchemaGuard – Intelligent Parquet Schema Evolution & Data Quality Pipeline

## Overview

SchemaGuard is a Python-based data engineering project designed to process Parquet data batches while handling schema evolution, data quality validation, invalid records, late-arriving data, and pipeline monitoring.

The pipeline detects schema changes, accepts compatible changes, quarantines breaking changes, validates records, transforms valid data, and maintains execution metadata using SQLite.

## Key Features

- **Batch Ingestion:** Reads and processes Parquet files in batches.
- **Schema Drift Detection:** Detects changes in incoming data structures.
- **Schema Evolution:** Supports compatible changes, such as adding nullable columns.
- **Breaking Schema Detection:** Identifies incompatible changes and quarantines affected batches.
- **Data Quality Validation:** Checks null values, duplicate IDs, invalid amounts, statuses, and customer references.
- **Quarantine Management:** Separates invalid records and rejected batches for further review.
- **Late Arrival Handling:** Identifies late-arriving records using a configured watermark.
- **Idempotent Processing:** Skips previously processed filenames.
- **Pipeline Monitoring:** Tracks execution history, batch status, audit logs, and metrics.
- **REST API:** Exposes pipeline operations through FastAPI.
- **Dashboard:** Provides pipeline monitoring through Streamlit.

## Technology Stack

| Category | Technologies |
|---|---|
| Programming Language | Python |
| Data Processing | Pandas, PyArrow |
| File Format | Parquet |
| Database | SQLite |
| Backend API | FastAPI |
| Frontend Dashboard | Streamlit |
| Testing | Pytest |
| Version Control | Git, GitHub |

## Project Structure

```text
SchemaGuard_Data_Pipeline/
│
├── backend/
│   ├── api/
│   │   └── routes/
│   ├── pipeline/
│   ├── services/
│   └── database/
│
├── frontend/
│   ├── app.py
│   ├── pages/
│   └── components/
│
├── scripts/
│   └── generate_data.py
│
├── tests/
│   ├── unit/
│   └── integration/
│
├── docs/
├── main.py
├── requirements.txt
├── pytest.ini
├── .gitignore
└── README.md
```

## Installation and Setup

### 1. Clone the repository

```bash
git clone https://github.com/SusmithaRayapati/SchemaGuard_Data_Pipeline.git
cd SchemaGuard_Data_Pipeline
```

### 2. Create a virtual environment

```bash
python -m venv venv
```

### 3. Activate the environment

Windows PowerShell:

```powershell
.\venv\Scripts\Activate.ps1
```

### 4. Install dependencies

```bash
pip install -r requirements.txt
```

### 5. Generate sample data

```bash
python scripts/generate_data.py
```

### 6. Run the pipeline

```bash
python main.py
```

## Run the FastAPI Backend

```bash
uvicorn backend.main:app --reload
```

API documentation:

http://127.0.0.1:8000/docs

## Run the Streamlit Dashboard

```bash
streamlit run frontend/app.py
```

Dashboard URL:

http://localhost:8501

## Run Tests

```bash
python -m pytest -v
```

## Data Scenarios Covered

The project includes test scenarios for:

- Unchanged schema
- Compatible schema evolution
- Breaking schema changes
- Missing required columns
- Duplicate records
- Null and invalid values
- Invalid customer references
- Late-arriving records
- Empty input batches
- Malformed Parquet files
- Large-volume data processing

## Pipeline Workflow

```text
Parquet Input Batches
        |
        v
Batch Ingestion
        |
        v
Schema Drift Detection
        |
        v
Schema Validation
        |
        v
Data Quality Validation
        |
        +------ Invalid Data ------> Quarantine
        |
        v
Data Transformation
        |
        v
Processed Parquet Output
        |
        v
SQLite Metadata & Metrics
        |
        v
FastAPI + Streamlit Dashboard
```

## Future Enhancements

- Content-hash-based idempotency
- Cloud storage integration
- Distributed data processing
- Advanced monitoring and alerting

## Author

**Susmitha Rayapati**

GitHub: [SusmithaRayapati](https://github.com/SusmithaRayapati)
