
"""
SchemaGuard - Database Connection

Responsibilities:
1. Create SQLite database.
2. Manage database connections.
3. Initialize database tables.
4. Handle transaction commit and rollback.
"""

from pathlib import Path
import sqlite3
from contextlib import contextmanager

from backend.database.models import SCHEMA_SQL


# --------------------------------------------------
# 1. DATABASE CONFIGURATION
# --------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATABASE_DIR = PROJECT_ROOT / "data"

DATABASE_PATH = DATABASE_DIR / "schemaguard.db"


# --------------------------------------------------
# 2. CREATE DATABASE CONNECTION
# --------------------------------------------------

def get_connection():
    """
    Create and return a SQLite database connection.
    """

    DATABASE_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    connection = sqlite3.connect(
        DATABASE_PATH,
        timeout=30
    )

    connection.row_factory = sqlite3.Row

    connection.execute(
        "PRAGMA foreign_keys = ON"
    )

    connection.execute(
        "PRAGMA busy_timeout = 30000"
    )

    return connection


# --------------------------------------------------
# 3. DATABASE SESSION
# --------------------------------------------------

@contextmanager
def database_session():
    """
    Manage database transactions.

    Successful execution:
        Commit transaction.

    Failed execution:
        Rollback transaction.

    Always:
        Close connection.
    """

    connection = get_connection()

    try:

        yield connection

        connection.commit()

    except Exception:

        connection.rollback()

        raise

    finally:

        connection.close()


# --------------------------------------------------
# 4. INITIALIZE DATABASE
# --------------------------------------------------

def initialize_database():
    """
    Create all required tables and indexes.
    """

    with database_session() as connection:

        connection.executescript(
            SCHEMA_SQL
        )

    print("\nSchemaGuard database initialized.")

    print(f"Database location: {DATABASE_PATH}")

    return DATABASE_PATH


# --------------------------------------------------
# 5. CHECK DATABASE CONNECTION
# --------------------------------------------------

def check_database_connection():

    try:

        with database_session() as connection:

            result = connection.execute(
                "SELECT sqlite_version()"
            ).fetchone()

            version = result[0]

        print("\nDatabase connection successful.")

        print(f"SQLite version: {version}")

        return True

    except sqlite3.Error as error:

        print(
            f"Database connection failed: {error}"
        )

        return False


# --------------------------------------------------
# 6. DIRECT EXECUTION
# --------------------------------------------------

if __name__ == "__main__":

    initialize_database()

    check_database_connection()
