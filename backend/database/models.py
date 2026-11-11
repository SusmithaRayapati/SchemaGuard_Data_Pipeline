
"""
SchemaGuard - Database Models

Defines SQLite tables used for:
1. Pipeline run tracking.
2. Batch metadata.
3. Quarantine logs.
4. Pipeline metrics.
"""

SCHEMA_SQL = """

CREATE TABLE IF NOT EXISTS pipeline_runs (

    run_id TEXT PRIMARY KEY,

    started_at TEXT NOT NULL,

    completed_at TEXT,

    status TEXT NOT NULL
        CHECK (
            status IN (
                'RUNNING',
                'SUCCESS',
                'FAILED',
                'PARTIAL'
            )
        ),

    total_files INTEGER DEFAULT 0,

    accepted_files INTEGER DEFAULT 0,

    quarantined_files INTEGER DEFAULT 0,

    total_input_records INTEGER DEFAULT 0,

    total_output_records INTEGER DEFAULT 0,

    total_rejected_records INTEGER DEFAULT 0,

    error_message TEXT
);


CREATE TABLE IF NOT EXISTS batch_metadata (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    run_id TEXT NOT NULL,

    source_file TEXT NOT NULL,

    schema_fingerprint TEXT,

    schema_status TEXT,

    batch_status TEXT NOT NULL,

    input_records INTEGER DEFAULT 0,

    valid_records INTEGER DEFAULT 0,

    rejected_records INTEGER DEFAULT 0,

    processed_file TEXT,

    error_message TEXT,

    created_at TEXT NOT NULL,

    FOREIGN KEY (run_id)
        REFERENCES pipeline_runs(run_id),

    UNIQUE(run_id, source_file)
);


CREATE TABLE IF NOT EXISTS quarantine_logs (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    quarantine_id TEXT NOT NULL UNIQUE,

    run_id TEXT NOT NULL,

    source_file TEXT NOT NULL,

    quarantine_type TEXT NOT NULL,

    quarantine_reason TEXT NOT NULL,

    record_count INTEGER DEFAULT 0,

    artifact_path TEXT,

    created_at TEXT NOT NULL,

    FOREIGN KEY (run_id)
        REFERENCES pipeline_runs(run_id)
);


CREATE TABLE IF NOT EXISTS pipeline_metrics (

    id INTEGER PRIMARY KEY AUTOINCREMENT,

    run_id TEXT NOT NULL,

    metric_name TEXT NOT NULL,

    metric_value REAL NOT NULL,

    unit TEXT,

    recorded_at TEXT NOT NULL,

    FOREIGN KEY (run_id)
        REFERENCES pipeline_runs(run_id)
);


CREATE INDEX IF NOT EXISTS idx_batch_run_id
ON batch_metadata(run_id);


CREATE INDEX IF NOT EXISTS idx_quarantine_run_id
ON quarantine_logs(run_id);


CREATE INDEX IF NOT EXISTS idx_metrics_run_id
ON pipeline_metrics(run_id);

CREATE TABLE IF NOT EXISTS processed_orders (

    order_id INTEGER PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TEXT NOT NULL,
    amount REAL NOT NULL,
    status TEXT NOT NULL,

    region TEXT,
    discount REAL DEFAULT 0,
    discount_amount REAL DEFAULT 0,
    net_amount REAL NOT NULL,

    order_year INTEGER,
    order_month INTEGER,

    source_file TEXT NOT NULL,
    run_id TEXT NOT NULL,

    loaded_at TEXT DEFAULT CURRENT_TIMESTAMP
);


CREATE INDEX IF NOT EXISTS idx_processed_customer
ON processed_orders(customer_id);


CREATE INDEX IF NOT EXISTS idx_processed_run
ON processed_orders(run_id);

"""
