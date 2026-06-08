"""
Task Intelligence SQLite -> Neon/Postgres import script.

Use this ONLY inside your Task_Intelligence_System_Cloud_Test folder.
It reads:
  - .env containing DATABASE_URL=...
  - task_flow_data/tasks.db
Then creates/replaces the cloud tables and imports your local data.
"""

import os
import sqlite3
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "task_flow_data"
SQLITE_DB = DATA_DIR / "tasks.db"

TABLES = ["subjects", "work_types", "priority_types", "assignees", "items"]

CREATE_SQL = {
    "subjects": """
        CREATE TABLE IF NOT EXISTS subjects (
            id BIGINT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        )
    """,
    "work_types": """
        CREATE TABLE IF NOT EXISTS work_types (
            id BIGINT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        )
    """,
    "priority_types": """
        CREATE TABLE IF NOT EXISTS priority_types (
            id BIGINT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL
        )
    """,
    "assignees": """
        CREATE TABLE IF NOT EXISTS assignees (
            id BIGINT PRIMARY KEY,
            name TEXT UNIQUE NOT NULL,
            active INTEGER DEFAULT 1
        )
    """,
    "items": """
        CREATE TABLE IF NOT EXISTS items (
            id BIGINT PRIMARY KEY,
            title TEXT,
            state TEXT,
            effort TEXT,
            work_type TEXT,
            item_type TEXT,
            subject TEXT,
            due TEXT,
            review_date TEXT,
            review_start TEXT,
            review_end TEXT,
            notes TEXT,
            project_id BIGINT,
            in_today INTEGER DEFAULT 0,
            created TEXT,
            priority_type TEXT DEFAULT 'As Needed',
            is_appointment INTEGER DEFAULT 0,
            appointment_date TEXT,
            is_reminder INTEGER DEFAULT 0,
            reminder_date TEXT,
            critical_due_date TEXT,
            assignee_id BIGINT,
            assignee_task_text TEXT,
            today_execution_order TEXT
        )
    """,
}


def sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
    ).fetchone()
    return row is not None


def sqlite_columns(conn: sqlite3.Connection, table: str):
    return [row[1] for row in conn.execute(f"PRAGMA table_info({table})").fetchall()]


def load_rows(conn: sqlite3.Connection, table: str):
    cols = sqlite_columns(conn, table)
    rows = conn.execute(f"SELECT * FROM {table}").fetchall()
    return cols, rows


def insert_rows(pg_cur, table: str, cols, rows):
    if not rows:
        return 0
    col_list = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    update_cols = [c for c in cols if c != "id"]
    update_sql = ", ".join([f"{c}=EXCLUDED.{c}" for c in update_cols])
    sql = f"""
        INSERT INTO {table} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {update_sql}
    """
    pg_cur.executemany(sql, [tuple(row) for row in rows])
    return len(rows)


def main():
    load_dotenv(BASE_DIR / ".env")
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise SystemExit("ERROR: DATABASE_URL was not found in .env")
    if not SQLITE_DB.exists():
        raise SystemExit(f"ERROR: SQLite database not found: {SQLITE_DB}")

    print("Using SQLite database:", SQLITE_DB)
    print("Connecting to Neon/Postgres...")

    sqlite_conn = sqlite3.connect(SQLITE_DB)
    sqlite_conn.row_factory = sqlite3.Row

    pg_conn = psycopg2.connect(database_url)
    pg_conn.autocommit = False

    try:
        with pg_conn.cursor() as cur:
            print("Creating cloud tables if needed...")
            for table in TABLES:
                cur.execute(CREATE_SQL[table])

            print("Clearing existing cloud rows...")
            for table in reversed(TABLES):
                cur.execute(f"DELETE FROM {table}")

            print("Importing rows...")
            for table in TABLES:
                if not sqlite_table_exists(sqlite_conn, table):
                    print(f"  - {table}: skipped, not found in SQLite")
                    continue
                cols, rows = load_rows(sqlite_conn, table)
                count = insert_rows(cur, table, cols, rows)
                print(f"  - {table}: imported {count} row(s)")

            pg_conn.commit()
            print("DONE: Import completed successfully.")
            print("Your local test app has not been changed by this import script.")
    except Exception:
        pg_conn.rollback()
        raise
    finally:
        sqlite_conn.close()
        pg_conn.close()


if __name__ == "__main__":
    main()
