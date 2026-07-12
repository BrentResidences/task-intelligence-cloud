
import os
import re
from datetime import datetime, date
from difflib import SequenceMatcher
from pathlib import Path
from io import BytesIO

import pandas as pd
import streamlit as st
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle

st.set_page_config(page_title="Task Management System", layout="wide")
# Version 27.5: cloud stability; each database operation uses a short-lived Neon connection so Streamlit reruns cannot share or deadlock one psycopg2 connection.
# Version 27.4: stable Neon connection; autocommit reads prevent idle transactions during page navigation.
# Version 27.3: no startup database maintenance; report query and PDF generation run only when explicitly requested.
# Version 27: adds Date Entered report filtering with optional start and optional end date. End Date can be None.
# Version 20: compact same-page navigation tabs; removes URL/link tabs so clicking tabs does not open new browser tabs.

DATA_DIR = Path("task_flow_data")
DATA_DIR.mkdir(exist_ok=True)
DB = DATA_DIR / "tasks.db"  # Local backup/export folder remains available.

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")
try:
    if not DATABASE_URL and "DATABASE_URL" in st.secrets:
        DATABASE_URL = st.secrets["DATABASE_URL"]
except Exception:
    pass


def open_new_pg_connection():
    """Open one short-lived Neon/Postgres connection.

    A Streamlit app reruns the script frequently and may also have more than one
    browser session active. A single globally cached psycopg2 connection can be
    used by overlapping reruns, which can block the app and make the host return
    a temporary 502 page. Each call now receives its own connection instead.
    """
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is missing. Add it in Streamlit Secrets or the local .env file.")

    raw = psycopg2.connect(
        DATABASE_URL,
        cursor_factory=DictCursor,
        connect_timeout=10,
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=3,
        application_name="task_intelligence_streamlit",
    )
    raw.autocommit = True
    return raw


def get_session_pg_connection():
    """Compatibility helper: return a new live connection for this operation."""
    return open_new_pg_connection()


def reset_session_pg_connection():
    """Compatibility helper retained for older calling code."""
    return None


WORK_TYPES = ["Unassigned", "Office", "Office - Email", "Office - Text", "Office - Call", "Ideas", "Driving", "Appointment"]
ITEM_TYPES = ["Task", "Ideas", "Concept", "Reminder", "Appointment"]
PRIORITY_TYPES = ["Today", "Soon", "Task Repository", "Scan / File"]
TODAY_EXECUTION_ORDERS = ["First", "Second", "Last"]
SUBJECTS = ['Unassigned', 'Accounting, Taxes, IRS - All companies', 'Billboards', 'Box Truck', 'Brent', 'Capital Improvements Master File', 'Carlos', 'Comfort West', 'Comfort West Capital Improvements', 'Contruction Crew Budgeting', 'Corey', 'Corey & Dalton', 'Dalton', 'Demolition', 'Draws - Current Month', 'Draws - Murfin Previous Month', 'Efficiency Duplexes', 'Emelie', 'Employee Loans', 'Employment', 'Employment Bonuses', 'Enegren', 'Equipment, Computers, Tools', 'Financing - Multi-family', 'Food / Workout', 'Gainsboro', 'Home Improvement  / Lisa Stuff', 'Home Improvements - Brent', 'Home Office', 'Hurst Remodeling LLC', 'HVAC', 'Insurance', 'KHRC Annual Reports', 'Labor Contacts', 'Land for Efficiency Duplexes, Small Homes', 'Landscaping & Mowing', 'Lisa Tasks', 'Lupe', 'Make Ready', 'Management', 'Management Company', 'Manufactured Housing Rentals', 'Marina Point', 'Materials Database', 'MH', 'MIR', 'Misc Tasks', 'Mom Tasks', 'Monday.com', 'Multi-family Renovation, Management Business', 'My Car', 'New Project - Existing MF', 'New Project - New Build', 'Phone calls', 'Plaza', 'Plaza Capital Improvements', 'Plaza Capital Improvements Budget', 'Plaza Operating Budget', 'Plaza Unit Renovation', 'Pool - Home', 'Prayer', 'Property Taxes', 'Realty Capital', 'Refinance For All Properties', 'Residences', 'Residences Capital Improvements', 'Residences Capital Improvements Budget', 'Residences Operating Budget', 'Residences Shop Project', 'Residences Wifi', 'ReVest Rentals', 'ReVest Rentals Capital Improvements', 'Roger', 'Sandstone', 'Sandstone Budget', 'Sandstone Capital Improvements', 'Sandstone Capital Improvements Budget', 'Sandstone Fire Repair', 'Sandstone Office Sign', 'Sandstone Office Suite 200', 'Sandstone Office Suite 500', 'Sandstone Operating Budget', 'Sandstone Trash', 'SCAD', 'Scheduling Tasks / Goals', 'Small Homes (Residences)', 'Software - Database, Tracking & Estimating', 'Staffing', 'Stafford Duplexes', 'Stafford Final Budget Template', 'Stafford Final Materials Cost Info', 'Steve Sonneman Partnership', 'Subcontractors', 'Subcontractors Insurance', 'Sugar Creek', 'Tax Returns & Financials', 'Trash Truck', 'Vineyards', 'Vineyards Capital Improvements', 'Vineyards Capital Improvements Budget', 'Vineyards Renovaton', 'Vineyards Shop', 'Westgate', 'Westgate Capital Improvements', 'Westgate Capital Improvements Budget', 'Work Comp']


class CloudCursor:
    def __init__(self, cursor):
        self.cursor = cursor

    def _convert_sql(self, sql):
        sql = str(sql)
        sql = re.sub(r"INSERT\s+OR\s+IGNORE\s+INTO", "INSERT INTO", sql, flags=re.IGNORECASE)
        add_on_conflict = bool(re.search(r"INSERT\s+INTO", sql, flags=re.IGNORECASE)) and "ON CONFLICT" not in sql.upper()
        sql = sql.replace("?", "%s")
        if add_on_conflict:
            sql = sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
        return sql

    def execute(self, sql, params=None):
        return self.cursor.execute(self._convert_sql(sql), params)

    def executemany(self, sql, params=None):
        return self.cursor.executemany(self._convert_sql(sql), params)

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def __iter__(self):
        return iter(self.cursor)

    def __getattr__(self, name):
        return getattr(self.cursor, name)


class CloudConnection:
    def __init__(self):
        self.raw = open_new_pg_connection()

    def cursor(self):
        return CloudCursor(self.raw.cursor())

    def execute(self, sql, params=None):
        cur = self.cursor()
        cur.execute(sql, params)
        return cur

    def _reconnect(self):
        try:
            if self.raw is not None and self.raw.closed == 0:
                self.raw.close()
        except Exception:
            pass
        self.raw = open_new_pg_connection()
        return self.raw

    def commit(self):
        try:
            return self.raw.commit()
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            self._reconnect()
            return None

    def rollback(self):
        try:
            return self.raw.rollback()
        except (psycopg2.InterfaceError, psycopg2.OperationalError):
            self._reconnect()
            return None

    def close(self):
        try:
            if self.raw is not None and self.raw.closed == 0:
                self.raw.close()
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
        return False

    def __getattr__(self, name):
        return getattr(self.raw, name)


def conn():
    # Cloud/iPhone version: each operation gets its own short-lived connection.
    return CloudConnection()


def invalidate_caches():
    try:
        st.cache_data.clear()
    except Exception:
        pass


def invalidate_item_caches():
    """Clear only item/list/report caches for fast Quick Add and Today updates.

    This avoids reloading stable dropdown lists (subjects, work types, priorities,
    assignees) every time a single item is added, completed, or reordered.
    """
    for fn_name in [
        "load_today",
        "load_all",
        "load_completed",
        "load_projects",
        "load_projects_and_concepts",
        "project_name_map",
        "dashboard_counts",
        "load_weekly_queue",
        "load_fast_reschedule_items",
    ]:
        try:
            globals()[fn_name].clear()
        except Exception:
            pass


def ensure_subjects_table():
    with conn() as c:
        cur = c.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subjects(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
        """)
        c.commit()


def seed_subjects_if_empty():
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM subjects")
        count = cur.fetchone()[0]
        if count == 0:
            for s in SUBJECTS:
                if s and s != "Unassigned":
                    try:
                        cur.execute("INSERT INTO subjects (name) VALUES (?)", (s,))
                    except Exception:
                        pass
            c.commit()


@st.cache_data(show_spinner=False)
def get_subjects():
    with conn() as c:
        df = pd.read_sql("SELECT name FROM subjects ORDER BY LOWER(name)", c)
    vals = ["Unassigned"]
    if not df.empty:
        vals.extend(df["name"].tolist())
    return vals


def add_subject(name):
    name = (name or "").strip()
    if not name:
        return False, "Subject name is required."
    if name == "Unassigned":
        return False, "That name is reserved."
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM subjects WHERE LOWER(name)=LOWER(?)", (name,))
        if cur.fetchone()[0] > 0:
            return False, "Subject already exists."
        cur.execute("INSERT INTO subjects (name) VALUES (?)", (name,))
        c.commit()
    invalidate_caches()
    return True, "Subject added."


def rename_subject(old_name, new_name):
    old_name = (old_name or "").strip()
    new_name = (new_name or "").strip()
    if not old_name or not new_name:
        return False, "Both subject names are required."
    if old_name == "Unassigned":
        return False, "Unassigned cannot be renamed."
    if new_name == "Unassigned":
        return False, "That name is reserved."
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM subjects WHERE LOWER(name)=LOWER(?)", (new_name,))
        exists = cur.fetchone()[0]
        if exists > 0 and old_name.lower() != new_name.lower():
            return False, "A subject with that name already exists."
        cur.execute("UPDATE subjects SET name=? WHERE name=?", (new_name, old_name))
        cur.execute("UPDATE items SET subject=? WHERE subject=?", (new_name, old_name))
        c.commit()
    invalidate_caches()
    return True, "Subject renamed."


def delete_subject(name, replacement="Unassigned"):
    name = (name or "").strip()
    replacement = (replacement or "").strip() or "Unassigned"
    if not name:
        return False, "Subject name is required."
    if name == "Unassigned":
        return False, "Unassigned cannot be deleted."
    if replacement == name:
        replacement = "Unassigned"
    with conn() as c:
        cur = c.cursor()
        if replacement != "Unassigned":
            cur.execute("SELECT COUNT(*) FROM subjects WHERE name=?", (replacement,))
            if cur.fetchone()[0] == 0:
                return False, "Replacement subject does not exist."
        cur.execute("UPDATE items SET subject=? WHERE subject=?", (replacement, name))
        cur.execute("DELETE FROM subjects WHERE name=?", (name,))
        c.commit()
    invalidate_caches()
    return True, "Subject deleted."



def ensure_work_types_table():
    with conn() as c:
        cur = c.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS work_types(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
        """)
        c.commit()


def seed_work_types_if_empty():
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM work_types")
        count = cur.fetchone()[0]
        if count == 0:
            for w in WORK_TYPES:
                if w and w != "Unassigned":
                    try:
                        cur.execute("INSERT INTO work_types (name) VALUES (?)", (w,))
                    except Exception:
                        pass
            c.commit()




def sync_priority_and_work_type_values():
    """Keep category lists aligned with the current simplified workflow."""
    with conn() as c:
        cur = c.cursor()

        # Keep existing records usable after replacing Normal/Backlog.
        cur.execute("UPDATE items SET priority_type='Task Repository' WHERE priority_type IS NULL OR TRIM(priority_type)='' OR priority_type IN ('Normal', 'Backlog', 'As Needed')")

        # Rename the old manager-list priority so existing cloud data uses the new label.
        try:
            cur.execute("UPDATE priority_types SET name='Task Repository' WHERE name='As Needed'")
            cur.execute("DELETE FROM priority_types WHERE name='As Needed'")
            cur.execute("INSERT OR IGNORE INTO priority_types (name) VALUES ('Task Repository')")
        except Exception:
            pass

        # Rename old work type values into the new Office sub-types / Driving category.
        work_type_mappings = {
            'Call': 'Office - Call',
            'Text': 'Office - Text',
            'Email': 'Office - Email',
            'Errand': 'Driving',
        }
        for old_name, new_name in work_type_mappings.items():
            cur.execute("UPDATE items SET work_type=? WHERE work_type=?", (new_name, old_name))

        # Ensure the new work type choices exist.
        for name in WORK_TYPES:
            if name and name != 'Unassigned':
                cur.execute("INSERT OR IGNORE INTO work_types (name) VALUES (?)", (name,))

        # Remove obsolete work type choices so they no longer appear in dropdowns.
        cur.execute("DELETE FROM work_types WHERE name IN ('Call', 'Text', 'Email', 'Errand')")
        c.commit()

@st.cache_data(show_spinner=False)
def get_work_types():
    with conn() as c:
        df = pd.read_sql("SELECT name FROM work_types ORDER BY LOWER(name)", c)
    vals = ["Unassigned"]
    if not df.empty:
        vals.extend(df["name"].tolist())
    return vals


def add_work_type(name):
    name = (name or "").strip()
    if not name:
        return False, "Work type name is required."
    if name == "Unassigned":
        return False, "That name is reserved."
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM work_types WHERE LOWER(name)=LOWER(?)", (name,))
        if cur.fetchone()[0] > 0:
            return False, "Work type already exists."
        cur.execute("INSERT INTO work_types (name) VALUES (?)", (name,))
        c.commit()
    return True, "Work type added."


def rename_work_type(old_name, new_name):
    old_name = (old_name or "").strip()
    new_name = (new_name or "").strip()
    if not old_name or not new_name:
        return False, "Both work type names are required."
    if old_name == "Unassigned":
        return False, "Unassigned cannot be renamed."
    if new_name == "Unassigned":
        return False, "That name is reserved."
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM work_types WHERE LOWER(name)=LOWER(?)", (new_name,))
        exists = cur.fetchone()[0]
        if exists > 0 and old_name.lower() != new_name.lower():
            return False, "A work type with that name already exists."
        cur.execute("UPDATE work_types SET name=? WHERE name=?", (new_name, old_name))
        cur.execute("UPDATE items SET work_type=? WHERE work_type=?", (new_name, old_name))
        c.commit()
    return True, "Work type renamed."


def delete_work_type(name, replacement="Unassigned"):
    name = (name or "").strip()
    replacement = (replacement or "").strip() or "Unassigned"
    if not name:
        return False, "Work type name is required."
    if name == "Unassigned":
        return False, "Unassigned cannot be deleted."
    if replacement == name:
        replacement = "Unassigned"
    with conn() as c:
        cur = c.cursor()
        if replacement != "Unassigned":
            cur.execute("SELECT COUNT(*) FROM work_types WHERE name=?", (replacement,))
            if cur.fetchone()[0] == 0:
                return False, "Replacement work type does not exist."
        cur.execute("UPDATE items SET work_type=? WHERE work_type=?", (replacement, name))
        cur.execute("DELETE FROM work_types WHERE name=?", (name,))
        c.commit()
    return True, "Work type deleted."



def ensure_priority_types_table():
    with conn() as c:
        cur = c.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS priority_types(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
        """)
        c.commit()


def seed_priority_types_if_empty():
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM priority_types")
        count = cur.fetchone()[0]
        if count == 0:
            for p in PRIORITY_TYPES:
                if p:
                    try:
                        cur.execute("INSERT INTO priority_types (name) VALUES (?)", (p,))
                    except Exception:
                        pass
            c.commit()


@st.cache_data(show_spinner=False)
def get_priority_types():
    with conn() as c:
        df = pd.read_sql("SELECT name FROM priority_types ORDER BY id", c)
    vals = []
    if not df.empty:
        vals.extend(df["name"].tolist())
    if not vals:
        vals = PRIORITY_TYPES.copy()
    return vals


def add_priority_type(name):
    name = (name or "").strip()
    if not name:
        return False, "Priority name is required."
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM priority_types WHERE LOWER(name)=LOWER(?)", (name,))
        if cur.fetchone()[0] > 0:
            return False, "Priority already exists."
        cur.execute("INSERT INTO priority_types (name) VALUES (?)", (name,))
        c.commit()
    invalidate_caches()
    return True, "Priority added."


def rename_priority_type(old_name, new_name):
    old_name = (old_name or "").strip()
    new_name = (new_name or "").strip()
    if not old_name or not new_name:
        return False, "Both priority names are required."
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM priority_types WHERE LOWER(name)=LOWER(?)", (new_name,))
        exists = cur.fetchone()[0]
        if exists > 0 and old_name.lower() != new_name.lower():
            return False, "A priority with that name already exists."
        cur.execute("UPDATE priority_types SET name=? WHERE name=?", (new_name, old_name))
        cur.execute("UPDATE items SET priority_type=? WHERE priority_type=?", (new_name, old_name))
        c.commit()
    invalidate_caches()
    return True, "Priority renamed."


def delete_priority_type(name, replacement="Task Repository"):
    name = (name or "").strip()
    replacement = (replacement or "").strip() or "Task Repository"
    if not name:
        return False, "Priority name is required."
    if replacement == name:
        replacement = "Task Repository"
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM priority_types WHERE name=?", (replacement,))
        if cur.fetchone()[0] == 0:
            return False, "Replacement priority does not exist."
        cur.execute("UPDATE items SET priority_type=? WHERE priority_type=?", (replacement, name))
        cur.execute("DELETE FROM priority_types WHERE name=?", (name,))
        c.commit()
    invalidate_caches()
    return True, "Priority deleted."


def ensure_assignees_table():
    with conn() as c:
        cur = c.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS assignees(
                id INTEGER PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                active INTEGER DEFAULT 1
            )
        """)
        c.commit()


@st.cache_data(show_spinner=False)
def get_assignees_df(include_inactive=False):
    with conn() as c:
        if include_inactive:
            df = pd.read_sql("SELECT id, name, active FROM assignees ORDER BY LOWER(name)", c)
        else:
            df = pd.read_sql("SELECT id, name, active FROM assignees WHERE COALESCE(active, 1)=1 ORDER BY LOWER(name)", c)
    return df


def get_assignee_options(include_unassigned=True, include_inactive=False):
    df = get_assignees_df(include_inactive=include_inactive)
    options = ["Unassigned"] if include_unassigned else []
    if not df.empty:
        options.extend(df["name"].tolist())
    return options


def assignee_name_to_id(name):
    name = (name or "").strip()
    if not name or name == "Unassigned":
        return None
    df = get_assignees_df(include_inactive=True)
    if df.empty:
        return None
    match = df[df["name"] == name]
    if match.empty:
        return None
    return int(match.iloc[0]["id"])


def assignee_id_to_name(assignee_id):
    try:
        if assignee_id is None or str(assignee_id).strip() == "":
            return "Unassigned"
        assignee_id = int(assignee_id)
    except Exception:
        return "Unassigned"
    df = get_assignees_df(include_inactive=True)
    if df.empty:
        return "Unassigned"
    match = df[df["id"] == assignee_id]
    if match.empty:
        return "Unassigned"
    return str(match.iloc[0]["name"])


def add_assignee(name):
    name = (name or "").strip()
    if not name:
        return False, "Assignee name is required."
    if name == "Unassigned":
        return False, "That name is reserved."
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM assignees WHERE LOWER(name)=LOWER(?)", (name,))
        if cur.fetchone()[0] > 0:
            return False, "Assignee already exists."
        cur.execute("INSERT INTO assignees (name, active) VALUES (?, 1)", (name,))
        c.commit()
    invalidate_caches()
    return True, "Assignee added."


def rename_assignee(old_name, new_name):
    old_name = (old_name or "").strip()
    new_name = (new_name or "").strip()
    if not old_name or not new_name:
        return False, "Both assignee names are required."
    if old_name == "Unassigned":
        return False, "Unassigned cannot be renamed."
    if new_name == "Unassigned":
        return False, "That name is reserved."
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COUNT(*) FROM assignees WHERE LOWER(name)=LOWER(?)", (new_name,))
        exists = cur.fetchone()[0]
        if exists > 0 and old_name.lower() != new_name.lower():
            return False, "An assignee with that name already exists."
        cur.execute("UPDATE assignees SET name=?, active=1 WHERE name=?", (new_name, old_name))
        c.commit()
    invalidate_caches()
    return True, "Assignee renamed."


def delete_assignee(name):
    name = (name or "").strip()
    if not name:
        return False, "Assignee name is required."
    if name == "Unassigned":
        return False, "Unassigned cannot be deleted."
    assignee_id = assignee_name_to_id(name)
    if assignee_id is None:
        return False, "Assignee not found."
    with conn() as c:
        cur = c.cursor()
        cur.execute("UPDATE items SET assignee_id=NULL WHERE assignee_id=?", (assignee_id,))
        cur.execute("DELETE FROM assignees WHERE id=?", (assignee_id,))
        c.commit()
    invalidate_caches()
    return True, "Assignee deleted and existing assigned items were set to Unassigned."


def assign_items_to_assignee(item_ids, assignee_name):
    assignee_id = assignee_name_to_id(assignee_name)
    with conn() as c:
        cur = c.cursor()
        for item_id in item_ids:
            cur.execute("UPDATE items SET assignee_id=? WHERE id=?", (assignee_id, int(item_id)))
        c.commit()
    invalidate_item_caches()


def save_assignee_task_text(item_id, assignee_task_text):
    with conn() as c:
        c.execute("UPDATE items SET assignee_task_text=? WHERE id=?", ((assignee_task_text or "").strip(), int(item_id)))
        c.commit()
    invalidate_item_caches()


def add_delegated_task(title, assignee_name, work_type="Unassigned", subject="Unassigned", priority_type="Task Repository", notes="", due="", is_appointment=0, appointment_date="", is_reminder=0, reminder_date="", critical_due_date="", assignee_task_text=""):
    title = (title or "").strip()
    if not title:
        return False, "Task text is required."
    assignee_id = assignee_name_to_id(assignee_name)
    if assignee_id is None:
        return False, "Choose an assignee before saving the task."
    available_work_types = get_work_types()
    final_work_type = work_type if work_type in available_work_types and work_type != "Unassigned" else detect_work_type(title)
    available_subjects = get_subjects()
    final_subject = subject if subject in available_subjects else "Unassigned"
    available_priority_types = get_priority_types()
    final_priority_type = priority_type if priority_type in available_priority_types else "Task Repository"
    final_assignee_text = (assignee_task_text or "").strip() or title
    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM items")
        next_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO items (id, title, state, effort, work_type, item_type, subject, priority_type, due, review_date, review_start, review_end, notes, project_id, is_appointment, appointment_date, is_reminder, reminder_date, critical_due_date, assignee_id, assignee_task_text, created, in_today)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            """,
            (next_id, title, "Open", "Medium", final_work_type, "Task", final_subject, final_priority_type, due, "", "", "", notes, None, int(bool(is_appointment)), appointment_date, int(bool(is_reminder)), reminder_date, critical_due_date, assignee_id, final_assignee_text, str(datetime.now()))
        )
        c.commit()
    invalidate_item_caches()
    return True, f"Assigned task #{next_id} to {assignee_name}."


def save_today_execution_order(item_id, today_execution_order):
    chosen_order = today_execution_order if today_execution_order in TODAY_EXECUTION_ORDERS else "Last"
    with conn() as c:
        c.execute("UPDATE items SET today_execution_order=? WHERE id=?", (chosen_order, int(item_id)))
        c.commit()
    invalidate_item_caches()


def build_assignee_report_df(assignee_name="All", priority_filter="All", subject_filter="All", include_completed=False):
    df = pd.read_sql("""
        SELECT items.*, assignees.name AS assignee_name
        FROM items
        LEFT JOIN assignees ON items.assignee_id = assignees.id
        WHERE COALESCE(items.item_type, 'Task') NOT IN ('Project', 'Ideas')
    """, conn())
    if df.empty:
        return df
    df = df.copy()
    if not include_completed:
        df = df[df["state"].fillna("Open") != "Complete"].copy()
    if assignee_name != "All":
        if assignee_name == "Unassigned":
            df = df[df["assignee_id"].isna()].copy()
        else:
            df = df[df["assignee_name"].fillna("Unassigned") == assignee_name].copy()
    if priority_filter != "All":
        df = df[df["priority_type"].fillna("Task Repository") == priority_filter].copy()
    if subject_filter != "All":
        df = df[df["subject"].fillna("Unassigned") == subject_filter].copy()
    df["Assignee"] = df["assignee_name"].fillna("Unassigned")
    df["Item ID"] = df["id"]
    df["Task Text"] = df.apply(lambda r: str(r.get("assignee_task_text", "") or "").strip() if str(r.get("assignee_task_text", "") or "").strip() else str(r.get("title", "") or ""), axis=1)
    df["Master Item Title"] = df["title"].fillna("")
    df["Subject"] = df["subject"].fillna("Unassigned")
    df["Priority"] = df["priority_type"].fillna("Task Repository")
    df["Critical Due Date"] = df["critical_due_date"].apply(format_date_safe)
    df["Appointment"] = df.apply(lambda r: format_date_safe(r.get("appointment_date", "")) if bool_from_db(r.get("is_appointment", 0)) else "", axis=1)
    df["Reminder"] = df.apply(lambda r: format_date_safe(r.get("reminder_date", "")) if bool_from_db(r.get("is_reminder", 0)) else "", axis=1)
    return df.sort_values(by=["Assignee", "Priority", "Subject", "Item ID"], ascending=[True, True, True, True])


def build_assignee_worklist_pdf(report_df, report_title="Assignee Work List"):
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.45 * inch,
        leftMargin=0.45 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.45 * inch,
    )
    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    normal_style = styles["BodyText"]
    normal_style.fontSize = 8
    normal_style.leading = 10
    story = []
    story.append(Paragraph(pdf_escape(report_title), title_style))
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph(f"Rows printed: {0 if report_df is None else len(report_df)}", styles["Normal"]))
    story.append(Spacer(1, 0.12 * inch))

    if report_df is None or report_df.empty:
        story.append(Paragraph("No assigned items matched the selected filters.", normal_style))
    else:
        table_data = [[
            Paragraph("<b>Assignee</b>", normal_style),
            Paragraph("<b>Item ID</b>", normal_style),
            Paragraph("<b>Task Text</b>", normal_style),
            Paragraph("<b>Subject</b>", normal_style),
            Paragraph("<b>Priority</b>", normal_style),
            Paragraph("<b>Critical Due</b>", normal_style),
            Paragraph("<b>Appt / Reminder</b>", normal_style),
        ]]
        for _, row in report_df.iterrows():
            appt_rem = []
            if str(row.get("Appointment", "") or "").strip():
                appt_rem.append(f"Appt: {row.get('Appointment', '')}")
            if str(row.get("Reminder", "") or "").strip():
                appt_rem.append(f"Reminder: {row.get('Reminder', '')}")
            table_data.append([
                Paragraph(pdf_escape(row.get("Assignee", "")), normal_style),
                Paragraph(pdf_escape(row.get("Item ID", "")), normal_style),
                Paragraph(pdf_escape(row.get("Task Text", "")), normal_style),
                Paragraph(pdf_escape(row.get("Subject", "")), normal_style),
                Paragraph(pdf_escape(row.get("Priority", "")), normal_style),
                Paragraph(pdf_escape(row.get("Critical Due Date", "")), normal_style),
                Paragraph(pdf_escape(" | ".join(appt_rem)), normal_style),
            ])

        table = Table(
            table_data,
            colWidths=[0.85 * inch, 0.48 * inch, 3.0 * inch, 1.15 * inch, 0.75 * inch, 0.7 * inch, 0.9 * inch],
            repeatRows=1,
        )
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 4),
            ("RIGHTPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ]))
        story.append(table)

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


def ensure_schema():
    with conn() as c:
        cur = c.cursor()
        cur.execute(
            '''
            CREATE TABLE IF NOT EXISTS items(
                id INTEGER PRIMARY KEY,
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
                project_id INTEGER,
                in_today INTEGER DEFAULT 0,
                created TEXT
            )
            '''
        )
        # PostgreSQL/Neon compatible replacement for SQLite PRAGMA table_info(items).
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = 'items'
        """)
        cols = [row[0] for row in cur.fetchall()]
        additions = {
            "item_type": "TEXT DEFAULT 'Task'",
            "subject": "TEXT DEFAULT 'Unassigned'",
            "priority_type": "TEXT DEFAULT 'Task Repository'",
            "due": "TEXT",
            "review_date": "TEXT",
            "review_start": "TEXT",
            "review_end": "TEXT",
            "notes": "TEXT",
            "project_id": "INTEGER",
            "is_appointment": "INTEGER DEFAULT 0",
            "appointment_date": "TEXT",
            "is_reminder": "INTEGER DEFAULT 0",
            "reminder_date": "TEXT",
            "critical_due_date": "TEXT",
            "assignee_id": "INTEGER",
            "assignee_task_text": "TEXT",
            "today_execution_order": "TEXT"
        }
        for col, col_type in additions.items():
            if col not in cols:
                cur.execute(f"ALTER TABLE items ADD COLUMN {col} {col_type}")
        c.commit()


def detect_work_type(text):
    t = (text or "").lower()
    if "call" in t:
        return "Office - Call"
    if "email" in t:
        return "Office - Email"
    if any(x in t for x in ["text ", " text", "sms", "message ", " message"]):
        return "Office - Text"
    if any(x in t for x in ["bank", "review", "scan", "file", "filing", "paperwork", "invoice"]):
        return "Office"
    if any(x in t for x in ["pick up", "drop off", "buy", "drive", "deliver", "pickup"]):
        return "Driving"
    if any(x in t for x in ["meeting", "appointment", "inspection"]):
        return "Appointment"
    return "Ideas"


def detect_item_type(text):
    t = (text or "").lower()
    if any(x in t for x in ["appointment", "meeting", "inspection"]):
        return "Appointment"
    if any(w in t for w in ["idea", "concept", "think about", "consider", "possible", "maybe", "someday"]):
        return "Concept"
    if any(w in t for w in ["remind", "remember", "follow up", "revisit", "check back", "bring back"]):
        return "Reminder"
    if any(w in t for w in ["plan", "planning", "project", "system", "replacement", "renovation", "build out", "strategy"]):
        return "Ideas"
    return "Task"


def backfill():
    with conn() as c:
        rows = c.execute(
            "SELECT id, title, item_type, work_type, subject, priority_type, notes, due, review_date, review_start, review_end, critical_due_date, today_execution_order FROM items"
        ).fetchall()
        for row in rows:
            if row["item_type"] is None or str(row["item_type"]).strip() == "":
                c.execute("UPDATE items SET item_type=? WHERE id=?", (detect_item_type(row["title"]), row["id"]))
            if row["work_type"] is None or str(row["work_type"]).strip() in ("", "Unassigned"):
                c.execute("UPDATE items SET work_type=? WHERE id=?", (detect_work_type(row["title"]), row["id"]))
            if row["subject"] is None or str(row["subject"]).strip() == "":
                c.execute("UPDATE items SET subject='Unassigned' WHERE id=?", (row["id"],))
            if row["priority_type"] is None or str(row["priority_type"]).strip() == "":
                c.execute("UPDATE items SET priority_type='Task Repository' WHERE id=?", (row["id"],))
            try:
                if row["today_execution_order"] is None or str(row["today_execution_order"]).strip() == "":
                    c.execute("UPDATE items SET today_execution_order='Last' WHERE id=?", (row["id"],))
            except Exception:
                pass
            for col in ["notes", "due", "review_date", "review_start", "review_end", "critical_due_date"]:
                if row[col] is None:
                    c.execute(f"UPDATE items SET {col}='' WHERE id=?", (row["id"],))
        c.commit()


def parse_date_safe(value):
    """Return a date object from the mixed date values stored in SQLite/Neon/pandas.

    The cloud report may receive dates as:
    - Python date/datetime objects
    - pandas Timestamp values
    - strings like 2026-04-21
    - strings like 2026-04-21 13:45:00
    - strings like 04/21/2026

    V23 was too strict and could turn valid Date Entered values blank.
    """
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value

    s = str(value).strip()
    if not s:
        return None

    # Try pandas first because it handles timestamps and common database strings.
    try:
        parsed = pd.to_datetime(s, errors="coerce")
        if not pd.isna(parsed):
            return parsed.date()
    except Exception:
        pass

    # Fallback explicit formats.
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            return datetime.strptime(s[:19], fmt).date()
        except Exception:
            pass
    return None


def format_date_safe(value):
    if value is None or value == "":
        return ""
    if isinstance(value, date):
        return value.strftime("%m/%d/%Y")
    parsed = parse_date_safe(value)
    if parsed is not None:
        return parsed.strftime("%m/%d/%Y")
    return ""


def format_date_report(value):
    """Short report date format like 4-21-26."""
    if value is None or value == "":
        return ""
    parsed = parse_date_safe(value)
    if parsed is None and isinstance(value, date):
        parsed = value
    if parsed is None:
        return ""
    return f"{parsed.month}-{parsed.day}-{str(parsed.year)[-2:]}"


def report_sort_date(value):
    parsed = parse_date_safe(value)
    if parsed is None and isinstance(value, date):
        parsed = value
    return parsed or date(1900, 1, 1)


def bool_from_db(value):
    try:
        return int(value or 0) == 1
    except Exception:
        return False


def appointment_reminder_status(row):
    bits = []
    try:
        is_appt = bool_from_db(row.get("is_appointment", 0))
        appt_date = row.get("appointment_date", "")
        is_rem = bool_from_db(row.get("is_reminder", 0))
        rem_date = row.get("reminder_date", "")
    except Exception:
        is_appt = bool_from_db(getattr(row, "is_appointment", 0))
        appt_date = getattr(row, "appointment_date", "")
        is_rem = bool_from_db(getattr(row, "is_reminder", 0))
        rem_date = getattr(row, "reminder_date", "")
    if is_appt:
        bits.append("Appointment" + (f": {format_date_safe(appt_date)}" if format_date_safe(appt_date) else ""))
    if is_rem:
        bits.append("Reminder" + (f": {format_date_safe(rem_date)}" if format_date_safe(rem_date) else ""))
    try:
        critical_due_date = row.get("critical_due_date", "")
    except Exception:
        critical_due_date = getattr(row, "critical_due_date", "")
    if format_date_safe(critical_due_date):
        bits.append(f"Critical Due: {format_date_safe(critical_due_date)}")
    return bits

def pdf_escape(value):
    text = str(value or "")
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_subjects_pdf():
    subjects = sorted(get_subjects(), key=lambda x: str(x).lower())
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.5 * inch,
        leftMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
    )
    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    normal_style = styles["BodyText"]
    normal_style.fontSize = 10
    normal_style.leading = 13

    story = []
    story.append(Paragraph("Current Subjects", title_style))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph(f"Total subjects: {len(subjects)}", styles["Normal"]))
    story.append(Spacer(1, 0.15 * inch))

    table_data = [[Paragraph("<b>Subject</b>", normal_style)]]
    for subject in subjects:
        table_data.append([Paragraph(pdf_escape(subject), normal_style)])

    table = Table(table_data, colWidths=[7.25 * inch], repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(table)

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


def due_status(due_value):
    d = parse_date_safe(due_value)
    if d is None:
        return ""
    today = date.today()
    delta = (d - today).days
    if delta < 0:
        return "OVERDUE"
    if delta == 0:
        return "DUE TODAY"
    if delta <= 3:
        return "DUE SOON"
    return ""


def review_status(review_value):
    d = parse_date_safe(review_value)
    if d is None:
        return ""
    today = date.today()
    delta = (d - today).days
    if delta < 0:
        return "REVIEW OVERDUE"
    if delta == 0:
        return "REVIEW TODAY"
    if delta <= 3:
        return "REVIEW SOON"
    return ""


def review_window_status(start_value, end_value):
    start_dt = parse_date_safe(start_value)
    end_dt = parse_date_safe(end_value)
    today = date.today()
    if start_dt is None and end_dt is None:
        return ""
    if start_dt and today < start_dt:
        return "REVIEW WINDOW FUTURE"
    if start_dt and end_dt and start_dt <= today <= end_dt:
        return "REVIEW WINDOW ACTIVE"
    if start_dt and end_dt is None and today >= start_dt:
        return "REVIEW WINDOW ACTIVE"
    if end_dt and today > end_dt:
        return "REVIEW WINDOW OVERDUE"
    if end_dt and start_dt is None and today <= end_dt:
        return "REVIEW WINDOW ACTIVE"
    return ""


# IMPORTANT: do not run schema changes, seed operations, migrations, or backfills
# during normal Streamlit startup or page navigation. The cloud database is already
# established. One-time database maintenance must be run manually from a dedicated
# maintenance tool, never from the normal application startup path.
#
# The following functions remain available in the source for controlled maintenance,
# but none of them are called automatically here:
#   ensure_schema(), ensure_subjects_table(), ensure_work_types_table(),
#   ensure_priority_types_table(), ensure_assignees_table(),
#   seed_subjects_if_empty(), seed_work_types_if_empty(),
#   seed_priority_types_if_empty(), sync_priority_and_work_type_values(), backfill().


def add_item(title, item_type="Task", work_type="Unassigned", subject="Unassigned", priority_type="Task Repository", notes="", project_id=None,
             due="", review_date="", review_start="", review_end="", is_appointment=0, appointment_date="", is_reminder=0, reminder_date="", critical_due_date="", assignee_id=None):
    final_item_type = item_type if item_type in ITEM_TYPES else detect_item_type(title)
    available_work_types = get_work_types()
    final_work_type = work_type if work_type in available_work_types and work_type != "Unassigned" else detect_work_type(title)
    available_subjects = get_subjects()
    final_subject = subject if subject in available_subjects else "Unassigned"
    available_priority_types = get_priority_types()
    final_priority_type = priority_type if priority_type in available_priority_types else "Task Repository"

    with conn() as c:
        cur = c.cursor()
        cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM items")
        next_id = cur.fetchone()[0]
        cur.execute(
            '''
            INSERT INTO items (id, title, state, effort, work_type, item_type, subject, priority_type, due, review_date, review_start, review_end, notes, project_id, is_appointment, appointment_date, is_reminder, reminder_date, critical_due_date, assignee_id, created, in_today)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ''',
            (next_id, title, "Open", "Medium", final_work_type, final_item_type, final_subject, final_priority_type, due, review_date, review_start, review_end, notes, project_id, int(bool(is_appointment)), appointment_date, int(bool(is_reminder)), reminder_date, critical_due_date, assignee_id, str(datetime.now()))
        )
    invalidate_item_caches()


def save_item(item_id, title, item_type, work_type, subject, priority_type, notes, project_id, due, review_date, review_start, review_end, is_appointment=None, appointment_date=None, is_reminder=None, reminder_date=None, critical_due_date=None, assignee_id=None):
    with conn() as c:
        c.execute(
            '''
            UPDATE items
            SET title=?, item_type=?, work_type=?, subject=?, priority_type=?, notes=?, project_id=?, due=?, review_date=?, review_start=?, review_end=?,
                is_appointment=COALESCE(?, is_appointment), appointment_date=COALESCE(?, appointment_date),
                is_reminder=COALESCE(?, is_reminder), reminder_date=COALESCE(?, reminder_date),
                critical_due_date=COALESCE(?, critical_due_date),
                assignee_id=COALESCE(?, assignee_id)
            WHERE id=?
            ''',
            (title, item_type, work_type, subject, priority_type, notes, project_id, due, review_date, review_start, review_end,
             None if is_appointment is None else int(bool(is_appointment)), appointment_date,
             None if is_reminder is None else int(bool(is_reminder)), reminder_date, critical_due_date, assignee_id, item_id)
        )
    invalidate_item_caches()


def delete_item(item_id):
    """Delete one item from Neon and immediately clear item/list caches."""
    item_id = int(item_id)
    with conn() as c:
        cur = c.cursor()
        # If this item is an idea/project header, keep child items but detach them.
        cur.execute("UPDATE items SET project_id=NULL WHERE project_id=?", (item_id,))
        cur.execute("DELETE FROM items WHERE id=?", (item_id,))
        c.commit()
    invalidate_item_caches()
    return item_id


def delete_item_and_stop(item_id):
    """Delete and intentionally stop the rerun before Streamlit redraws hundreds of widgets."""
    deleted_id = delete_item(item_id)
    st.session_state["_fast_delete_done_id"] = int(deleted_id)
    st.session_state["_stop_after_delete"] = True


def clear_today():
    with conn() as c:
        c.execute("UPDATE items SET in_today=0")
    invalidate_item_caches()


def remove_item(item_id):
    with conn() as c:
        c.execute("UPDATE items SET in_today=0, priority_type='Task Repository' WHERE id=?", (item_id,))
    invalidate_item_caches()


def add_to_today(item_id):
    with conn() as c:
        c.execute("UPDATE items SET in_today=1 WHERE id=?", (item_id,))
    invalidate_item_caches()


def mark_complete(item_id):
    with conn() as c:
        c.execute("UPDATE items SET state='Complete', in_today=0 WHERE id=?", (item_id,))
    invalidate_item_caches()


def mark_open(item_id):
    with conn() as c:
        c.execute("UPDATE items SET state='Open' WHERE id=?", (item_id,))
    invalidate_item_caches()


def generate(n=5, include_project_filler=True):
    df = pd.read_sql("SELECT * FROM items WHERE in_today=0 AND COALESCE(state, 'Open') != 'Complete' AND COALESCE(item_type, 'Task') NOT IN ('Project', 'Ideas')", conn())
    if df.empty:
        return

    today = date.today()
    actionable = df[(df["item_type"].isin(["Task", "Reminder", "Appointment"])) & (df["priority_type"].fillna("Task Repository") != "Scan / File")].copy()
    forced = []
    forced_ids = set()

    def add_forced_rows(rows_df):
        for _, row in rows_df.iterrows():
            if len(forced) >= n:
                break
            rid = int(row["id"])
            if rid not in forced_ids:
                forced.append(rid)
                forced_ids.add(rid)

    if not actionable.empty:
        actionable["due_dt"] = actionable["due"].apply(parse_date_safe)
        actionable["review_dt"] = actionable["review_date"].apply(parse_date_safe)

        overdue_due = actionable[actionable["due_dt"].apply(lambda x: x is not None and x < today)]
        due_today = actionable[actionable["due_dt"].apply(lambda x: x is not None and x == today)]
        overdue_review = actionable[actionable["review_dt"].apply(lambda x: x is not None and x <= today)]

        add_forced_rows(overdue_due.sort_values(by="due"))
        add_forced_rows(due_today.sort_values(by="due"))
        add_forced_rows(overdue_review.sort_values(by="review_date"))

    actionable = actionable[~actionable["id"].isin(list(forced_ids))].copy() if not actionable.empty else actionable

    if len(forced) < n and not actionable.empty:
        actionable["work_type"] = actionable["work_type"].fillna("Unassigned")
        actionable.loc[actionable["work_type"] == "Unassigned", "work_type"] = actionable["title"].apply(detect_work_type)

        selected = list(forced)
        groups = ["Appointment", "Office - Call", "Office - Text", "Office - Email", "Office", "Ideas", "Driving"]

        for g in groups:
            if len(selected) >= n:
                break
            sub = actionable[actionable["work_type"] == g]
            if not sub.empty:
                chosen_id = int(sub.iloc[0]["id"])
                selected.append(chosen_id)
                actionable = actionable[actionable["id"] != chosen_id]

        remaining_needed = max(0, n - len(selected))
        if remaining_needed > 0 and not actionable.empty:
            for i in actionable.head(remaining_needed).itertuples():
                selected.append(int(i.id))
    else:
        selected = list(forced)

    if len(selected) < n:
        backlog_df = df[df["priority_type"].fillna("Task Repository") == "Scan / File"].copy()
        if not backlog_df.empty:
            backlog_df["priority_score"] = backlog_df.apply(priority_score_row, axis=1)
            backlog_df = backlog_df.sort_values(by=["priority_score", "id"], ascending=[False, False])
            for _, row in backlog_df.iterrows():
                rid = int(row["id"])
                if rid not in selected:
                    selected.append(rid)
                    break

    if include_project_filler and len(selected) < n:
        project_df = df[df["item_type"].isin(["Project", "Ideas"])].copy()
        if not project_df.empty:
            project_df["review_dt"] = project_df["review_date"].apply(parse_date_safe)
            project_df["window_status"] = project_df.apply(
                lambda r: review_window_status(r["review_start"], r["review_end"]), axis=1
            )

            project_priority = pd.concat([
                project_df[project_df["window_status"] == "REVIEW WINDOW OVERDUE"],
                project_df[project_df["window_status"] == "REVIEW WINDOW ACTIVE"],
                project_df[project_df["review_dt"].apply(lambda x: x is not None and x <= date.today())],
                project_df
            ]).drop_duplicates(subset=["id"])

            for _, row in project_priority.iterrows():
                rid = int(row["id"])
                if rid not in selected:
                    selected.append(rid)
                    if len(selected) >= n:
                        break

    with conn() as c:
        for item_id in selected[:n]:
            c.execute("UPDATE items SET in_today=1 WHERE id=?", (item_id,))
    invalidate_caches()


@st.cache_data(show_spinner=False)
def load_today():
    return pd.read_sql("""
        SELECT *,
               CASE COALESCE(today_execution_order, 'Last')
                   WHEN 'First' THEN 1
                   WHEN 'Second' THEN 2
                   ELSE 3
               END AS today_order_sort
        FROM items
        WHERE (
              LOWER(TRIM(COALESCE(priority_type, 'Task Repository'))) = 'today'
              OR COALESCE(in_today, 0) = 1
          )
          AND COALESCE(state, 'Open') != 'Complete'
          AND COALESCE(item_type, 'Task') NOT IN ('Project', 'Ideas')
        ORDER BY today_order_sort, work_type, subject, title, id
    """, conn())


@st.cache_data(show_spinner=False)
def load_all():
    return pd.read_sql("SELECT * FROM items WHERE COALESCE(state, 'Open') != 'Complete' AND COALESCE(item_type, 'Task') NOT IN ('Project', 'Ideas') ORDER BY id DESC", conn())


@st.cache_data(show_spinner=False)
def load_completed():
    return pd.read_sql("SELECT * FROM items WHERE COALESCE(state, 'Open') = 'Complete' ORDER BY id DESC", conn())


@st.cache_data(show_spinner=False)
def load_projects():
    return pd.read_sql("SELECT * FROM items WHERE item_type IN ('Project', 'Ideas') AND COALESCE(state, 'Open') != 'Complete' AND (project_id IS NULL OR project_id=0) ORDER BY id DESC", conn())


@st.cache_data(show_spinner=False)
def load_project_children(project_id):
    return pd.read_sql("SELECT * FROM items WHERE project_id=? AND COALESCE(state, 'Open') != 'Complete' ORDER BY id DESC", conn(), params=(project_id,))


@st.cache_data(show_spinner=False)
def load_projects_and_concepts():
    return pd.read_sql("SELECT * FROM items WHERE COALESCE(state, 'Open') != 'Complete' AND ((item_type IN ('Project', 'Ideas') AND (project_id IS NULL OR project_id=0)) OR item_type='Concept') ORDER BY item_type, id DESC", conn())


@st.cache_data(show_spinner=False)
def project_name_map():
    df = load_projects()
    mapping = {0: "None"}
    for r in df.itertuples():
        mapping[int(r.id)] = r.title
    return mapping


@st.cache_data(show_spinner=False)
def dashboard_counts():
    df = load_all()
    today = date.today()
    due_today = overdue = review_due = active_review_windows = overdue_review_windows = 0
    for _, row in df.iterrows():
        d = parse_date_safe(row.get("due", ""))
        r = parse_date_safe(row.get("review_date", ""))
        window = review_window_status(row.get("review_start", ""), row.get("review_end", ""))
        if d is not None:
            if d < today:
                overdue += 1
            elif d == today:
                due_today += 1
        if r is not None and r <= today:
            review_due += 1
        if window == "REVIEW WINDOW ACTIVE":
            active_review_windows += 1
        elif window == "REVIEW WINDOW OVERDUE":
            overdue_review_windows += 1
    return overdue, due_today, review_due, active_review_windows, overdue_review_windows



def week_window():
    today = date.today()
    start = today - timedelta(days=today.weekday())
    end = start + timedelta(days=6)
    return start, end


def weekly_status(row):
    today = date.today()
    start, end = week_window()

    due_dt = parse_date_safe(row.get("due", ""))
    review_dt = parse_date_safe(row.get("review_date", ""))
    review_start_dt = parse_date_safe(row.get("review_start", ""))
    review_end_dt = parse_date_safe(row.get("review_end", ""))

    flags = []

    if due_dt is not None:
        if due_dt < today:
            flags.append("OVERDUE")
        elif start <= due_dt <= end:
            flags.append("DUE THIS WEEK")

    if review_dt is not None:
        if review_dt <= today:
            flags.append("REVIEW DUE")
        elif start <= review_dt <= end:
            flags.append("REVIEW THIS WEEK")

    if review_start_dt is not None or review_end_dt is not None:
        window = review_window_status(row.get("review_start", ""), row.get("review_end", ""))
        if window == "REVIEW WINDOW ACTIVE":
            flags.append("ACTIVE WINDOW")
        elif window == "REVIEW WINDOW OVERDUE":
            flags.append("WINDOW OVERDUE")
        elif review_start_dt is not None and start <= review_start_dt <= end:
            flags.append("WINDOW STARTS THIS WEEK")

    return flags


@st.cache_data(show_spinner=False)
def load_weekly_queue():
    df = load_all()
    if df.empty:
        return df
    df = df[df["state"].fillna("Open") != "Complete"]

    start, end = week_window()
    today = date.today()

    def in_week_or_relevant(row):
        due_dt = parse_date_safe(row.get("due", ""))
        review_dt = parse_date_safe(row.get("review_date", ""))
        review_start_dt = parse_date_safe(row.get("review_start", ""))
        review_end_dt = parse_date_safe(row.get("review_end", ""))

        if due_dt is not None and (due_dt < today or start <= due_dt <= end):
            return True
        if review_dt is not None and (review_dt <= today or start <= review_dt <= end):
            return True

        window = review_window_status(row.get("review_start", ""), row.get("review_end", ""))
        if window in ("REVIEW WINDOW ACTIVE", "REVIEW WINDOW OVERDUE"):
            return True

        if review_start_dt is not None and start <= review_start_dt <= end:
            return True
        if review_end_dt is not None and start <= review_end_dt <= end:
            return True

        return False

    weekly_df = df[df.apply(in_week_or_relevant, axis=1)].copy()
    return weekly_df


def add_week_queue_to_today(max_items=5):
    weekly_df = load_weekly_queue()
    if weekly_df.empty:
        return 0

    current_today = set(load_today()["id"].tolist()) if not load_today().empty else set()
    weekly_df = weekly_df[~weekly_df["id"].isin(current_today)].copy()

    if weekly_df.empty:
        return 0

    def priority_score(row):
        score = 0
        due_dt = parse_date_safe(row.get("due", ""))
        review_dt = parse_date_safe(row.get("review_date", ""))
        today = date.today()

        if row.get("item_type") == "Appointment":
            score += 100
        if row.get("item_type") == "Reminder":
            score += 40
        if row.get("item_type") == "Task":
            score += 20
        if row.get("item_type") in ("Project", "Ideas"):
            score += 10

        if due_dt is not None:
            if due_dt < today:
                score += 120
            elif due_dt == today:
                score += 100
            elif due_dt <= today + timedelta(days=7):
                score += 50

        if review_dt is not None:
            if review_dt <= today:
                score += 80
            elif review_dt <= today + timedelta(days=7):
                score += 30

        window = review_window_status(row.get("review_start", ""), row.get("review_end", ""))
        if window == "REVIEW WINDOW OVERDUE":
            score += 90
        elif window == "REVIEW WINDOW ACTIVE":
            score += 45

        wt = row.get("work_type", "")
        if wt == "Office - Call":
            score += 8
        elif wt == "Office - Email":
            score += 6
        elif wt == "Office":
            score += 5

        return score

    weekly_df["priority_score"] = weekly_df.apply(priority_score, axis=1)
    weekly_df = weekly_df.sort_values(by=["priority_score", "id"], ascending=[False, False])

    added = 0
    for _, row in weekly_df.head(max_items).iterrows():
        add_to_today(int(row["id"]))
        added += 1

    return added


from datetime import timedelta


def priority_score_row(row):
    today = date.today()
    score = 0

    item_type = row.get("item_type", "") or ""
    work_type = row.get("work_type", "") or ""
    subject = row.get("subject", "") or ""
    due_dt = parse_date_safe(row.get("due", ""))
    review_dt = parse_date_safe(row.get("review_date", ""))
    review_start_dt = parse_date_safe(row.get("review_start", ""))
    review_end_dt = parse_date_safe(row.get("review_end", ""))

    # Item type
    if item_type == "Appointment":
        score += 140
    elif item_type == "Reminder":
        score += 80
    elif item_type == "Task":
        score += 50
    elif item_type in ("Project", "Ideas"):
        score += 30
    elif item_type == "Concept":
        score += 10

    # Due urgency
    if due_dt is not None:
        delta = (due_dt - today).days
        if delta < 0:
            score += 180
        elif delta == 0:
            score += 150
        elif delta <= 3:
            score += 90
        elif delta <= 7:
            score += 45

    # Review urgency
    if review_dt is not None:
        delta = (review_dt - today).days
        if delta < 0:
            score += 110
        elif delta == 0:
            score += 90
        elif delta <= 3:
            score += 45
        elif delta <= 7:
            score += 20

    # Review window
    window = review_window_status(row.get("review_start", ""), row.get("review_end", ""))
    if window == "REVIEW WINDOW OVERDUE":
        score += 120
    elif window == "REVIEW WINDOW ACTIVE":
        score += 70
    elif window == "REVIEW WINDOW FUTURE":
        if review_start_dt is not None:
            delta = (review_start_dt - today).days
            if delta <= 7:
                score += 15

    # Work type weighting
    if work_type == "Office - Call":
        score += 10
    elif work_type == "Office - Text":
        score += 9
    elif work_type == "Office - Email":
        score += 8
    elif work_type == "Office":
        score += 7
    elif work_type == "Driving":
        score += 6
    elif work_type == "Ideas":
        score += 5

    # Already attached to a project -> useful structure
    if row.get("project_id", None) not in (None, "", 0):
        score += 8

    # Subject assigned -> clearer item
    if subject and subject != "Unassigned":
        score += 4

    # Keyword-based action bias
    title = (row.get("title", "") or "").lower()
    for word in ["call", "text", "email", "review", "check", "follow up", "schedule", "drop off", "pick up"]:
        if word in title:
            score += 3

    return score


def balanced_pick(df, n=5):
    if df.empty:
        return []

    work_type_limits = {
        "Appointment": 2,
        "Office - Call": 2,
        "Office - Text": 2,
        "Office - Email": 2,
        "Office": 2,
        "Ideas": 2,
        "Driving": 2,
        "Unassigned": 1,
    }
    item_type_limits = {
        "Appointment": 2,
        "Reminder": 2,
        "Task": 4,
        "Ideas": 2,
        "Concept": 1,
    }

    picked = []
    picked_ids = set()
    work_counts = {}
    item_counts = {}

    def can_take(row):
        wt = row.get("work_type", "Unassigned") or "Unassigned"
        it = row.get("item_type", "Task") or "Task"
        if work_counts.get(wt, 0) >= work_type_limits.get(wt, 2):
            return False
        if item_counts.get(it, 0) >= item_type_limits.get(it, 3):
            return False
        return True

    # Pass 1: take highest scoring while respecting balance
    for _, row in df.iterrows():
        if len(picked) >= n:
            break
        rid = int(row["id"])
        if rid in picked_ids:
            continue
        if can_take(row):
            picked.append(rid)
            picked_ids.add(rid)
            wt = row.get("work_type", "Unassigned") or "Unassigned"
            it = row.get("item_type", "Task") or "Task"
            work_counts[wt] = work_counts.get(wt, 0) + 1
            item_counts[it] = item_counts.get(it, 0) + 1

    # Pass 2: fill remainder regardless of limits
    if len(picked) < n:
        for _, row in df.iterrows():
            if len(picked) >= n:
                break
            rid = int(row["id"])
            if rid in picked_ids:
                continue
            picked.append(rid)
            picked_ids.add(rid)

    return picked


def add_scored_today(max_items=5):
    df = pd.read_sql("SELECT * FROM items WHERE in_today=0 AND COALESCE(state, 'Open') != 'Complete' AND COALESCE(item_type, 'Task') NOT IN ('Project', 'Ideas')", conn())
    if df.empty:
        return 0

    today = date.today()
    actionable = df[(df["item_type"].isin(["Task", "Reminder", "Appointment", "Ideas", "Project"])) & (df["priority_type"].fillna("Task Repository") != "Scan / File")].copy()
    if actionable.empty:
        return 0

    # Strongly prioritize urgent / active items
    def relevant_now(row):
        due_dt = parse_date_safe(row.get("due", ""))
        review_dt = parse_date_safe(row.get("review_date", ""))
        window = review_window_status(row.get("review_start", ""), row.get("review_end", ""))
        if due_dt is not None and due_dt <= today + timedelta(days=7):
            return True
        if review_dt is not None and review_dt <= today + timedelta(days=7):
            return True
        if window in ("REVIEW WINDOW ACTIVE", "REVIEW WINDOW OVERDUE"):
            return True
        return True

    actionable = actionable[actionable.apply(relevant_now, axis=1)].copy()
    actionable["priority_score"] = actionable.apply(priority_score_row, axis=1)
    actionable = actionable.sort_values(by=["priority_score", "id"], ascending=[False, False])

    target_normal = max_items - 1 if max_items > 1 else 1
    selected = balanced_pick(actionable, n=target_normal)

    backlog_df = df[df["priority_type"].fillna("Task Repository") == "Scan / File"].copy()
    if max_items > 1 and not backlog_df.empty:
        backlog_df["priority_score"] = backlog_df.apply(priority_score_row, axis=1)
        backlog_df = backlog_df.sort_values(by=["priority_score", "id"], ascending=[False, False])
        for _, row in backlog_df.iterrows():
            rid = int(row["id"])
            if rid not in selected:
                selected.append(rid)
                break

    if len(selected) < max_items:
        remaining = df[~df["id"].isin(selected)].copy()
        if not remaining.empty:
            remaining["priority_score"] = remaining.apply(priority_score_row, axis=1)
            remaining = remaining.sort_values(by=["priority_score", "id"], ascending=[False, False])
            for _, row in remaining.iterrows():
                rid = int(row["id"])
                if rid not in selected:
                    selected.append(rid)
                if len(selected) >= max_items:
                    break

    if not selected:
        return 0

    with conn() as c:
        for item_id in selected:
            c.execute("UPDATE items SET in_today=1 WHERE id=?", (item_id,))
    invalidate_caches()
    return len(selected)


def weekly_bucketed_rows():
    weekly_df = load_weekly_queue()
    if weekly_df.empty:
        return {"Urgent / Overdue": [], "Due This Week": [], "Review / Ideas Attention This Week": [], "Scan / File / Fill Work": []}

    weekly_df["priority_score"] = weekly_df.apply(priority_score_row, axis=1)
    weekly_df = weekly_df.sort_values(by=["priority_score", "id"], ascending=[False, False])

    urgent_rows, due_rows, review_rows, backlog_rows = [], [], [], []

    for _, row in weekly_df.iterrows():
        flags = weekly_status(row)
        item_type = row.get("item_type", "")

        if row.get("priority_type", "Task Repository") == "Scan / File":
            backlog_rows.append(row)
        elif "OVERDUE" in flags or "REVIEW DUE" in flags or "WINDOW OVERDUE" in flags:
            urgent_rows.append(row)
        elif "DUE THIS WEEK" in flags:
            due_rows.append(row)
        elif "REVIEW THIS WEEK" in flags or "ACTIVE WINDOW" in flags or "WINDOW STARTS THIS WEEK" in flags or item_type in ("Project", "Ideas"):
            review_rows.append(row)
        else:
            due_rows.append(row)

    return {
        "Urgent / Overdue": urgent_rows,
        "Due This Week": due_rows,
        "Review / Ideas Attention This Week": review_rows,
        "Scan / File / Fill Work": backlog_rows,
    }


def add_top_weekly_items_to_today(max_items=5):
    buckets = weekly_bucketed_rows()
    selected = []
    selected_ids = set()

    # Try to take from each bucket first
    for bucket_name in ["Urgent / Overdue", "Due This Week", "Review / Ideas Attention This Week", "Scan / File / Fill Work"]:
        rows = buckets[bucket_name]
        if rows:
            row = rows[0]
            rid = int(row["id"])
            if rid not in selected_ids:
                selected.append(rid)
                selected_ids.add(rid)
        if len(selected) >= max_items:
            break

    # Fill remainder from all rows combined by priority
    all_rows = []
    for rows in buckets.values():
        all_rows.extend(rows)

    if all_rows:
        seen = {}
        for row in all_rows:
            seen[int(row["id"])] = row
        combined = list(seen.values())
        combined.sort(key=lambda r: (priority_score_row(r), int(r["id"])), reverse=True)

        for row in combined:
            if len(selected) >= max_items:
                break
            rid = int(row["id"])
            if rid not in selected_ids:
                selected.append(rid)
                selected_ids.add(rid)

    if not selected:
        return 0

    current_today = set(load_today()["id"].tolist()) if not load_today().empty else set()
    added = 0
    with conn() as c:
        for item_id in selected:
            if item_id not in current_today:
                c.execute("UPDATE items SET in_today=1 WHERE id=?", (item_id,))
                added += 1
    invalidate_caches()
    return added


def normalize_text_for_similarity(text):
    text = (text or "").lower().strip()
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    stop_words = {
        "the","a","an","and","or","for","to","of","in","on","at","by","with",
        "about","regarding","re","my","me","this","that","it","is","be"
    }
    words = [w for w in text.split() if w not in stop_words]
    return " ".join(words)


def find_similar_items(new_text, limit=5):
    df = load_all()
    if df.empty or not new_text.strip():
        return pd.DataFrame()

    norm_new = normalize_text_for_similarity(new_text)
    if not norm_new:
        return pd.DataFrame()

    rows = []
    for _, row in df.iterrows():
        title = str(row.get("title", "") or "")
        norm_existing = normalize_text_for_similarity(title)
        if not norm_existing:
            continue

        ratio = SequenceMatcher(None, norm_new, norm_existing).ratio()

        new_words = set(norm_new.split())
        old_words = set(norm_existing.split())
        overlap = len(new_words & old_words)
        overlap_ratio = overlap / max(1, min(len(new_words), len(old_words)))

        score = max(ratio, overlap_ratio)

        if score >= 0.55 or overlap >= 2:
            rows.append({
                "id": row["id"],
                "title": title,
                "item_type": row.get("item_type", ""),
                "work_type": row.get("work_type", ""),
                "subject": row.get("subject", ""),
                "similarity_score": round(score, 3)
            })

    if not rows:
        return pd.DataFrame()

    result = pd.DataFrame(rows).sort_values(by=["similarity_score", "id"], ascending=[False, False]).head(limit)
    return result


def export_filtered_items_excel(df_export):
    export_path = DATA_DIR / "task_flow_all_items_export.xlsx"
    if df_export.empty:
        with pd.ExcelWriter(export_path, engine="openpyxl") as writer:
            pd.DataFrame(columns=["title"]).to_excel(writer, index=False, sheet_name="All Items")
    else:
        ordered_cols = [c for c in [
            "id","title","item_type","work_type","subject","state","effort","due","review_date",
            "review_start","review_end","project_id","notes","in_today","created"
        ] if c in df_export.columns]
        df_export = df_export[ordered_cols]
        with pd.ExcelWriter(export_path, engine="openpyxl") as writer:
            df_export.to_excel(writer, index=False, sheet_name="All Items")
    return export_path




def clearable_date_input(label, value, key_base):
    checkbox_key = f"{key_base}_enabled"
    date_key = f"{key_base}_date"

    default_enabled = value is not None
    enabled = st.checkbox(f"Use {label}", value=default_enabled, key=checkbox_key)

    if enabled:
        return st.date_input(label, value=value, key=date_key)
    else:
        return None


def title_text_area_height(value):
    text = str(value or "")
    # Approximate wrapped lines for the wide title box so long titles remain visible.
    estimated_lines = max(2, (len(text) // 110) + 1)
    return min(220, max(80, estimated_lines * 32))


if "quick_add_reset_counter" not in st.session_state:
    st.session_state.quick_add_reset_counter = 0
if "assignee_add_reset_counter" not in st.session_state:
    st.session_state.assignee_add_reset_counter = 0

def reset_quick_add_form():
    st.session_state.quick_add_reset_counter += 1
    st.session_state.pending_add_item = None

def reset_assignee_add_form():
    st.session_state.assignee_add_reset_counter += 1


def item_has_any_date(row):
    return any([
        str(row.get("due", "")).strip(),
        str(row.get("review_date", "")).strip(),
        str(row.get("review_start", "")).strip(),
        str(row.get("review_end", "")).strip(),
    ])


def date_match_reasons(row, target_date=None, start_date=None, end_date=None):
    reasons = []
    due_dt = parse_date_safe(row.get("due", ""))
    review_dt = parse_date_safe(row.get("review_date", ""))
    range_start_dt = parse_date_safe(row.get("review_start", ""))
    range_end_dt = parse_date_safe(row.get("review_end", ""))

    if target_date is not None:
        if due_dt is not None and due_dt == target_date:
            reasons.append(f"Due Date: {format_date_safe(due_dt)}")
        if review_dt is not None and review_dt == target_date:
            reasons.append(f"Review Date: {format_date_safe(review_dt)}")
        if range_start_dt is not None or range_end_dt is not None:
            if range_start_dt is not None and range_end_dt is not None and range_start_dt <= target_date <= range_end_dt:
                reasons.append(f"Date Range: {format_date_safe(range_start_dt)} to {format_date_safe(range_end_dt)}")
            elif range_start_dt is not None and range_end_dt is None and target_date >= range_start_dt:
                reasons.append(f"Date Range Start: {format_date_safe(range_start_dt)}")
            elif range_end_dt is not None and range_start_dt is None and target_date <= range_end_dt:
                reasons.append(f"Date Range End: {format_date_safe(range_end_dt)}")

    if start_date is not None and end_date is not None:
        if due_dt is not None and start_date <= due_dt <= end_date:
            reasons.append(f"Due Date: {format_date_safe(due_dt)}")
        if review_dt is not None and start_date <= review_dt <= end_date:
            reasons.append(f"Review Date: {format_date_safe(review_dt)}")
        if range_start_dt is not None or range_end_dt is not None:
            effective_start = range_start_dt if range_start_dt is not None else start_date
            effective_end = range_end_dt if range_end_dt is not None else end_date
            if effective_start <= end_date and effective_end >= start_date:
                if range_start_dt is not None and range_end_dt is not None:
                    reasons.append(f"Date Range: {format_date_safe(range_start_dt)} to {format_date_safe(range_end_dt)}")
                elif range_start_dt is not None:
                    reasons.append(f"Date Range Start: {format_date_safe(range_start_dt)}")
                elif range_end_dt is not None:
                    reasons.append(f"Date Range End: {format_date_safe(range_end_dt)}")

    return reasons


def build_exact_date_list(target_date):
    df = load_all()
    if df.empty:
        return pd.DataFrame()
    df = df[df.apply(item_has_any_date, axis=1)].copy()
    if df.empty:
        return pd.DataFrame()

    rows = []
    for _, row in df.iterrows():
        due_dt = parse_date_safe(row.get("due", ""))
        range_start_dt = parse_date_safe(row.get("review_start", ""))
        review_dt = parse_date_safe(row.get("review_date", ""))
        range_end_dt = parse_date_safe(row.get("review_end", ""))

        match_type = None
        if due_dt is not None and due_dt == target_date:
            match_type = "Firm Due Date"
        elif range_start_dt is not None and range_start_dt == target_date:
            match_type = "Date Range Start"

        if match_type is not None:
            reasons = []
            if due_dt is not None and due_dt == target_date:
                reasons.append(f"Due Date: {format_date_safe(due_dt)}")
            if review_dt is not None and review_dt == target_date:
                reasons.append(f"Review Date: {format_date_safe(review_dt)}")
            if range_start_dt is not None and range_start_dt == target_date:
                if range_end_dt is not None:
                    reasons.append(f"Date Range: {format_date_safe(range_start_dt)} to {format_date_safe(range_end_dt)}")
                else:
                    reasons.append(f"Date Range Start: {format_date_safe(range_start_dt)}")

            rows.append({
                "id": row.get("id", ""),
                "title": row.get("title", ""),
                "state": row.get("state", ""),
                "item_type": row.get("item_type", ""),
                "work_type": row.get("work_type", ""),
                "subject": row.get("subject", ""),
                "match_type": match_type,
                "match_priority": 0 if match_type == "Firm Due Date" else 1,
                "match_reason": " | ".join(reasons),
                "due": format_date_safe(row.get("due", "")),
                "review_date": format_date_safe(row.get("review_date", "")),
                "date_range_start": format_date_safe(row.get("review_start", "")),
                "date_range_end": format_date_safe(row.get("review_end", "")),
            })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(
        by=["match_priority", "state", "item_type", "work_type", "subject", "title"],
        ascending=True
    ).drop(columns=["match_priority"], errors="ignore")


def build_date_range_list(start_date, end_date):
    df = load_all()
    if df.empty:
        return pd.DataFrame()
    df = df[df.apply(item_has_any_date, axis=1)].copy()
    if df.empty:
        return pd.DataFrame()

    rows = []
    for _, row in df.iterrows():
        reasons = date_match_reasons(row, start_date=start_date, end_date=end_date)
        if reasons:
            rows.append({
                "id": row.get("id", ""),
                "title": row.get("title", ""),
                "state": row.get("state", ""),
                "item_type": row.get("item_type", ""),
                "work_type": row.get("work_type", ""),
                "subject": row.get("subject", ""),
                "match_reason": " | ".join(reasons),
                "due": format_date_safe(row.get("due", "")),
                "review_date": format_date_safe(row.get("review_date", "")),
                "date_range_start": format_date_safe(row.get("review_start", "")),
                "date_range_end": format_date_safe(row.get("review_end", "")),
            })
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows).sort_values(by=["state", "item_type", "work_type", "subject", "title"], ascending=True)



def build_date_range_summary_df(range_df):
    if range_df is None or range_df.empty:
        return pd.DataFrame(columns=["Due Date", "Item #", "Item Description"])

    summary_rows = []
    for _, row in range_df.iterrows():
        due_text = row.get("due", "") if str(row.get("due", "")).strip() else row.get("date_range_start", "")
        summary_rows.append({
            "Due Date": due_text,
            "Item #": row.get("id", ""),
            "Item Description": row.get("title", ""),
        })

    summary_df = pd.DataFrame(summary_rows)
    return summary_df.sort_values(by=["Due Date", "Item #", "Item Description"], ascending=True)


def build_date_range_summary_pdf(summary_df, start_date, end_date):
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.5 * inch,
        leftMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
    )
    styles = getSampleStyleSheet()
    story = []

    title_style = styles["Title"]
    normal_style = styles["BodyText"]
    normal_style.fontSize = 10
    normal_style.leading = 13

    story.append(Paragraph("Date Range Summary Report", title_style))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph(
        f"Range: {start_date.strftime('%m/%d/%Y')} to {end_date.strftime('%m/%d/%Y')}",
        styles["Heading3"]
    ))
    story.append(Spacer(1, 0.15 * inch))

    if summary_df.empty:
        story.append(Paragraph("No items matched that date range.", normal_style))
    else:
        table_data = [[
            Paragraph("<b>Due Date</b>", normal_style),
            Paragraph("<b>Item #</b>", normal_style),
            Paragraph("<b>Item Description</b>", normal_style),
        ]]
        for _, row in summary_df.iterrows():
            table_data.append([
                Paragraph(str(row.get("Due Date", "") or ""), normal_style),
                Paragraph(str(row.get("Item #", "") or ""), normal_style),
                Paragraph(str(row.get("Item Description", "") or ""), normal_style),
            ])

        table = Table(table_data, colWidths=[1.2 * inch, 0.8 * inch, 5.5 * inch], repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        story.append(table)

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


def build_exact_date_list_from_df(source_df, target_date):
    if source_df is None or source_df.empty:
        return pd.DataFrame()
    df = source_df[source_df.apply(item_has_any_date, axis=1)].copy()
    if df.empty:
        return pd.DataFrame()

    rows = []
    for _, row in df.iterrows():
        due_dt = parse_date_safe(row.get("due", ""))
        review_dt = parse_date_safe(row.get("review_date", ""))
        range_start_dt = parse_date_safe(row.get("review_start", ""))
        range_end_dt = parse_date_safe(row.get("review_end", ""))

        range_applies = False
        if range_start_dt is not None and range_end_dt is not None:
            range_applies = range_start_dt <= target_date <= range_end_dt
        elif range_start_dt is not None and range_end_dt is None:
            range_applies = target_date >= range_start_dt
        elif range_end_dt is not None and range_start_dt is None:
            range_applies = target_date <= range_end_dt

        match_type = None
        if due_dt is not None and due_dt == target_date:
            match_type = "Firm Due Date"
        elif range_applies:
            match_type = "Date Range"

        if match_type is not None:
            reasons = []
            if due_dt is not None and due_dt == target_date:
                reasons.append(f"Due Date: {format_date_safe(due_dt)}")
            if review_dt is not None and review_dt == target_date:
                reasons.append(f"Review Date: {format_date_safe(review_dt)}")
            if range_applies:
                if range_start_dt is not None and range_end_dt is not None:
                    reasons.append(f"Date Range: {format_date_safe(range_start_dt)} to {format_date_safe(range_end_dt)}")
                elif range_start_dt is not None:
                    reasons.append(f"Date Range Start: {format_date_safe(range_start_dt)}")
                elif range_end_dt is not None:
                    reasons.append(f"Date Range End: {format_date_safe(range_end_dt)}")

            rows.append({
                "id": row.get("id", ""),
                "title": row.get("title", ""),
                "state": row.get("state", ""),
                "item_type": row.get("item_type", ""),
                "work_type": row.get("work_type", ""),
                "subject": row.get("subject", ""),
                "match_type": match_type,
                "match_priority": 0 if match_type == "Firm Due Date" else 1,
                "match_reason": " | ".join(reasons),
                "due": format_date_safe(row.get("due", "")),
                "review_date": format_date_safe(row.get("review_date", "")),
                "date_range_start": format_date_safe(row.get("review_start", "")),
                "date_range_end": format_date_safe(row.get("review_end", "")),
            })

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows).sort_values(
        by=["match_priority", "state", "item_type", "work_type", "subject", "title"],
        ascending=True
    ).drop(columns=["match_priority"], errors="ignore")


def build_exact_date_summary_df(exact_df, include_range_items=True):
    if exact_df is None or exact_df.empty:
        return pd.DataFrame(columns=["Type", "Due Date", "Item #", "Item Description"])

    working_df = exact_df.copy()
    if not include_range_items and "match_type" in working_df.columns:
        working_df = working_df[working_df["match_type"] == "Firm Due Date"].copy()

    if working_df.empty:
        return pd.DataFrame(columns=["Type", "Due Date", "Item #", "Item Description"])

    summary_rows = []
    for _, row in working_df.iterrows():
        match_type = row.get("match_type", "")
        if match_type == "Firm Due Date":
            type_text = "Exact Due"
            due_text = row.get("due", "")
            match_priority = 0
        else:
            type_text = "Date Range"
            due_text = row.get("date_range_start", "")
            match_priority = 1

        summary_rows.append({
            "Type": type_text,
            "Due Date": due_text,
            "Item #": row.get("id", ""),
            "Item Description": row.get("title", ""),
            "_match_priority": match_priority,
        })

    summary_df = pd.DataFrame(summary_rows)
    return summary_df.sort_values(by=["_match_priority", "Due Date", "Item #", "Item Description"], ascending=True).drop(columns=["_match_priority"], errors="ignore")


def build_exact_date_summary_pdf(summary_df, exact_date, include_range_items=True, report_title="Exact Due Date Summary Report"):
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.5 * inch,
        leftMargin=0.5 * inch,
        topMargin=0.5 * inch,
        bottomMargin=0.5 * inch,
    )
    styles = getSampleStyleSheet()
    story = []

    title_style = styles["Title"]
    normal_style = styles["BodyText"]
    normal_style.fontSize = 9
    normal_style.leading = 11

    subtitle = f"Exact Date: {exact_date.strftime('%m/%d/%Y')}"
    if include_range_items:
        subtitle += " | Includes exact due date items first, then applicable date range items"
    else:
        subtitle += " | Exact due date items only"

    story.append(Paragraph(report_title, title_style))
    story.append(Spacer(1, 0.15 * inch))
    story.append(Paragraph(subtitle, styles["Heading3"]))
    story.append(Spacer(1, 0.15 * inch))

    if summary_df.empty:
        story.append(Paragraph("No items matched that exact date selection.", normal_style))
    else:
        table_data = [[
            Paragraph("<b>Type</b>", normal_style),
            Paragraph("<b>Due Date</b>", normal_style),
            Paragraph("<b>Item #</b>", normal_style),
            Paragraph("<b>Item Description</b>", normal_style),
        ]]
        for _, row in summary_df.iterrows():
            table_data.append([
                Paragraph(str(row.get("Type", "") or ""), normal_style),
                Paragraph(str(row.get("Due Date", "") or ""), normal_style),
                Paragraph(str(row.get("Item #", "") or ""), normal_style),
                Paragraph(str(row.get("Item Description", "") or ""), normal_style),
            ])

        table = Table(table_data, colWidths=[0.95 * inch, 1.0 * inch, 0.55 * inch, 5.0 * inch], repeatRows=1)
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 6),
            ("RIGHTPADDING", (0, 0), (-1, -1), 6),
            ("TOPPADDING", (0, 0), (-1, -1), 5),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
        ]))
        story.append(table)

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes



def build_current_filtered_driving_summary_df(driving_df, include_range_items=True):
    if driving_df is None or driving_df.empty:
        return pd.DataFrame(columns=["Type", "Due Date", "Item #", "Item Description"])

    summary_rows = []
    for _, row in driving_df.iterrows():
        due_dt = parse_date_safe(row.get("due", ""))
        range_start_dt = parse_date_safe(row.get("review_start", ""))
        range_end_dt = parse_date_safe(row.get("review_end", ""))

        if due_dt is not None:
            summary_rows.append({
                "Type": "Exact Due",
                "Due Date": format_date_safe(due_dt),
                "Item #": row.get("id", ""),
                "Item Description": row.get("title", ""),
                "_match_priority": 0,
            })
        elif include_range_items and (range_start_dt is not None or range_end_dt is not None):
            due_text = format_date_safe(range_start_dt) if range_start_dt is not None else format_date_safe(range_end_dt)
            summary_rows.append({
                "Type": "Date Range",
                "Due Date": due_text,
                "Item #": row.get("id", ""),
                "Item Description": row.get("title", ""),
                "_match_priority": 1,
            })

    if not summary_rows:
        return pd.DataFrame(columns=["Type", "Due Date", "Item #", "Item Description"])

    summary_df = pd.DataFrame(summary_rows)
    return summary_df.sort_values(
        by=["_match_priority", "Due Date", "Item #", "Item Description"],
        ascending=True
    ).drop(columns=["_match_priority"], errors="ignore")


def build_fast_reschedule_view_pdf(view_df, report_title="Fast Reschedule Current View"):
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(letter),
        rightMargin=0.35 * inch,
        leftMargin=0.35 * inch,
        topMargin=0.35 * inch,
        bottomMargin=0.35 * inch,
    )
    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    normal_style = styles["BodyText"]
    normal_style.fontSize = 7
    normal_style.leading = 9
    header_style = styles["BodyText"]
    header_style.fontSize = 7
    header_style.leading = 9

    story = []
    story.append(Paragraph(report_title, title_style))
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph(f"Rows printed: {0 if view_df is None else len(view_df)}", styles["Normal"]))
    story.append(Spacer(1, 0.12 * inch))

    table_data = [[
        Paragraph("<b>Item #</b>", header_style),
        Paragraph("<b>Title</b>", header_style),
        Paragraph("<b>Subject</b>", header_style),
        Paragraph("<b>Work Type</b>", header_style),
        Paragraph("<b>Appointment</b>", header_style),
        Paragraph("<b>Appointment Date</b>", header_style),
        Paragraph("<b>Reminder</b>", header_style),
        Paragraph("<b>Reminder Date</b>", header_style),
        Paragraph("<b>Critical Due</b>", header_style),
    ]]

    if view_df is not None and not view_df.empty:
        for _, row in view_df.iterrows():
            table_data.append([
                Paragraph(pdf_escape(row.get("ID", "")), normal_style),
                Paragraph(pdf_escape(row.get("Title", "")), normal_style),
                Paragraph(pdf_escape(row.get("Subject", "")), normal_style),
                Paragraph(pdf_escape(row.get("Work Type", "")), normal_style),
                Paragraph(pdf_escape("Yes" if bool_from_db(row.get("Appointment", False)) else ""), normal_style),
                Paragraph(pdf_escape(format_date_safe(normalize_editor_date_value(row.get("Appointment Date", "")))), normal_style),
                Paragraph(pdf_escape("Yes" if bool_from_db(row.get("Reminder", False)) else ""), normal_style),
                Paragraph(pdf_escape(format_date_safe(normalize_editor_date_value(row.get("Reminder Date", "")))), normal_style),
                Paragraph(pdf_escape(format_date_safe(normalize_editor_date_value(row.get("Critical Due Date", "")))), normal_style),
            ])

    table = Table(
        table_data,
        colWidths=[0.5 * inch, 3.05 * inch, 1.25 * inch, 0.8 * inch, 0.65 * inch, 0.8 * inch, 0.65 * inch, 0.8 * inch, 0.8 * inch],
        repeatRows=1,
    )
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.35, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 3),
        ("RIGHTPADDING", (0, 0), (-1, -1), 3),
        ("TOPPADDING", (0, 0), (-1, -1), 3),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
    ]))
    story.append(table)
    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


def build_ideas_print_pdf(ideas_df, subject_label="All"):
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=0.45 * inch,
        leftMargin=0.45 * inch,
        topMargin=0.45 * inch,
        bottomMargin=0.45 * inch,
    )
    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    normal_style = styles["BodyText"]
    normal_style.fontSize = 9
    normal_style.leading = 11
    header_style = styles["BodyText"]
    header_style.fontSize = 9
    header_style.leading = 11

    story = []
    story.append(Paragraph("Ideas Report", title_style))
    story.append(Spacer(1, 0.12 * inch))
    story.append(Paragraph(f"Subject: {pdf_escape(subject_label)}", styles["Heading3"]))
    story.append(Spacer(1, 0.12 * inch))

    table_data = [[
        Paragraph("<b>Idea #</b>", header_style),
        Paragraph("<b>Title</b>", header_style),
        Paragraph("<b>Subject</b>", header_style),
        Paragraph("<b>Notes / Description</b>", header_style),
    ]]

    if ideas_df is not None and not ideas_df.empty:
        for _, row in ideas_df.iterrows():
            table_data.append([
                Paragraph(pdf_escape(row.get("id", "")), normal_style),
                Paragraph(pdf_escape(row.get("title", "")), normal_style),
                Paragraph(pdf_escape(row.get("subject", "")), normal_style),
                Paragraph(pdf_escape(row.get("notes", "")), normal_style),
            ])

    table = Table(table_data, colWidths=[0.7 * inch, 2.25 * inch, 1.55 * inch, 3.0 * inch], repeatRows=1)
    table.setStyle(TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
        ("GRID", (0, 0), (-1, -1), 0.4, colors.grey),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("LEFTPADDING", (0, 0), (-1, -1), 4),
        ("RIGHTPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]))
    story.append(table)
    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes


def search_items_df(df, query):
    q = (query or "").strip().lower()
    if df.empty or not q:
        return pd.DataFrame()

    def row_matches(row):
        haystacks = [
            str(row.get("title", "") or "").lower(),
            str(row.get("notes", "") or "").lower(),
            str(row.get("subject", "") or "").lower(),
            str(row.get("work_type", "") or "").lower(),
        ]
        return any(q in h for h in haystacks)

    return df[df.apply(row_matches, axis=1)].copy()


def render_editable_item_results(df_items, prefix):
    if df_items.empty:
        st.info("No matching items found.")
        return

    current_proj_map = project_name_map()
    current_proj_reverse = {v: k for k, v in current_proj_map.items()}
    work_type_choices = get_work_types() if 'get_work_types' in globals() else WORK_TYPES

    for r in df_items.itertuples():
        with st.container(border=True):
            project_label = current_proj_map.get(int(r.project_id), "None") if pd.notna(r.project_id) and r.project_id else "None"
            subject_text = r.subject if pd.notna(r.subject) and str(r.subject).strip() else "Unassigned"
            priority_text = r.priority_type if pd.notna(r.priority_type) and str(r.priority_type).strip() else "Task Repository"
            status_bits = appointment_reminder_status(r)

            new_title = st.text_area(
                f"Title #{r.id}",
                value=r.title if r.title else "",
                key=f"{prefix}_title_{r.id}",
                height=title_text_area_height(r.title),
            )
            st.write("")
            st.caption(
                f"Work Type: {r.work_type} | Subject: {subject_text} | Priority: {priority_text} | Idea: {project_label}"
                + (f" | {' | '.join(status_bits)}" if status_bits else "")
            )

            ec1, ec2, ec3, ec4 = st.columns(4)
            new_item_type = r.item_type if str(r.item_type or "").strip() else "Task"
            with ec1:
                current_work_type = r.work_type if r.work_type in work_type_choices else "Unassigned"
                new_work_type = st.selectbox(f"Work Type #{r.id}", work_type_choices, index=work_type_choices.index(current_work_type), key=f"{prefix}_wt_{r.id}")
            with ec3:
                current_subject = r.subject if r.subject in get_subjects() else "Unassigned"
                new_subject = st.selectbox(f"Subject #{r.id}", get_subjects(), index=get_subjects().index(current_subject), key=f"{prefix}_subj_{r.id}")
            with ec4:
                priority_choices = get_priority_types()
                current_priority = r.priority_type if r.priority_type in priority_choices else "Task Repository"
                new_priority_type = st.selectbox(f"Priority #{r.id}", priority_choices, index=priority_choices.index(current_priority) if current_priority in priority_choices else 0, key=f"{prefix}_prio_{r.id}")

            notes_val = st.text_input(f"Notes #{r.id}", value=r.notes if pd.notna(r.notes) else "", key=f"{prefix}_notes_{r.id}")
            project_select = st.selectbox(
                f"Assign to Idea #{r.id}",
                list(current_proj_map.values()),
                index=list(current_proj_map.values()).index(project_label) if project_label in current_proj_map.values() else 0,
                key=f"{prefix}_proj_{r.id}"
            )
            critical_due_value = clearable_date_input(
                f"Critical Due Date #{r.id} (MM/DD/YYYY)",
                parse_date_safe(getattr(r, "critical_due_date", "")),
                f"{prefix}_critical_due_{r.id}"
            )

            a1, a2, a3, a4 = st.columns(4)
            with a1:
                if st.button("Save", key=f"{prefix}_save_{r.id}"):
                    chosen_project_id = current_proj_reverse.get(project_select, 0)
                    save_item(
                        r.id, new_title, new_item_type, new_work_type, new_subject, new_priority_type, notes_val,
                        None if chosen_project_id == 0 else chosen_project_id,
                        "", "", "", "",
                        critical_due_date=format_date_safe(critical_due_value)
                    )
                    st.rerun()
            with a2:
                if st.button("Add to Today", key=f"{prefix}_today_{r.id}"):
                    add_to_today(r.id)
                    st.rerun()
            with a3:
                if st.button("Complete", key=f"{prefix}_complete_{r.id}"):
                    mark_complete(r.id)
                    st.rerun()
            with a4:
                with st.expander("Delete", expanded=False):
                    st.warning("This permanently deletes this item.")
                    if st.button("Delete Item Now", key=f"{prefix}_delete_{r.id}", type="primary"):
                        delete_item(r.id)
                        st.rerun()


def invalidate_schedule_caches():
    for fn_name in [
        "load_all",
        "load_completed",
        "load_today",
        "load_projects",
        "load_project_children",
        "project_name_map",
        "load_projects_and_concepts",
        "dashboard_counts",
        "load_weekly_queue",
        "get_subjects",
        "get_priority_types",
        "load_fast_reschedule_items",
    ]:
        try:
            globals()[fn_name].clear()
        except Exception:
            pass


def save_dates_only(item_id, due, review_date, review_start, review_end):
    with conn() as c:
        c.execute(
            """
            UPDATE items
            SET due=?, review_date=?, review_start=?, review_end=?
            WHERE id=?
            """,
            (due, review_date, review_start, review_end, item_id)
        )
    invalidate_schedule_caches()


@st.cache_data(show_spinner=False)
def load_fast_reschedule_items():
    query = """
        SELECT id, title, state, item_type, work_type, subject, priority_type,
               due, review_date, review_start, review_end, project_id, in_today, notes,
               COALESCE(is_appointment, 0) AS is_appointment, appointment_date,
               COALESCE(is_reminder, 0) AS is_reminder, reminder_date, critical_due_date
        FROM items
        WHERE COALESCE(state, 'Open') != 'Complete'
          AND COALESCE(item_type, 'Task') NOT IN ('Project', 'Ideas')
        ORDER BY id DESC
    """
    return pd.read_sql(query, conn())


def render_fast_reschedule_results(df_items, prefix="fast_reschedule"):
    if df_items.empty:
        st.info("No matching items found.")
        return

    proj_map = project_name_map()

    for r in df_items.itertuples():
        with st.container(border=True):
            project_label = proj_map.get(int(r.project_id), "None") if pd.notna(r.project_id) and r.project_id else "None"
            status_bits = appointment_reminder_status(r)
            meta = [
                f"Work Type: {r.work_type}",
                f"Subject: {r.subject if pd.notna(r.subject) and str(r.subject).strip() else 'Unassigned'}",
                f"Idea: {project_label}",
            ]
            if getattr(r, "priority_type", "Task Repository") == "Scan / File":
                meta.append("Priority: Scan / File")
            if status_bits:
                meta.extend(status_bits)

            st.markdown(f"**{r.title if r.title else '(Untitled Item)'}**")
            st.caption(" | ".join(meta))

            b1, b2 = st.columns([1, 1])
            if b1.button("Add to Today", key=f"{prefix}_today_{r.id}"):
                add_to_today(r.id)
                st.rerun()
            if b2.button("Complete", key=f"{prefix}_complete_{r.id}"):
                mark_complete(r.id)
                invalidate_schedule_caches()
                st.rerun()



def normalize_editor_date_value(value):
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    if isinstance(value, pd.Timestamp):
        return value.date()
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    parsed = parse_date_safe(value)
    return parsed


def build_fast_reschedule_editor_df(df_items, proj_map):
    rows = []
    for _, row in df_items.iterrows():
        project_label = proj_map.get(int(row.get("project_id") or 0), "None") if pd.notna(row.get("project_id")) and row.get("project_id") else "None"
        notes_preview = str(row.get("notes", "") or "").strip()
        if len(notes_preview) > 120:
            notes_preview = notes_preview[:117] + "..."
        rows.append({
            "ID": int(row.get("id")),
            "Title": str(row.get("title", "") or ""),
            "Priority": str(row.get("priority_type", "") or "Task Repository"),
            "Subject": str(row.get("subject", "") or "Unassigned"),
            "Work Type": str(row.get("work_type", "") or ""),
            "Idea": project_label,
            "Appointment": bool_from_db(row.get("is_appointment", 0)),
            "Appointment Date": normalize_editor_date_value(row.get("appointment_date", "")),
            "Reminder": bool_from_db(row.get("is_reminder", 0)),
            "Reminder Date": normalize_editor_date_value(row.get("reminder_date", "")),
            "Critical Due Date": normalize_editor_date_value(row.get("critical_due_date", "")),
            "Complete": False,
            "Delete": False,
            "Notes Preview": notes_preview,
        })
    return pd.DataFrame(rows)



def sort_fast_reschedule_editor_df(editor_df, sort_by="ID", sort_direction="Ascending"):
    if editor_df is None or editor_df.empty:
        return editor_df
    if sort_by not in editor_df.columns:
        return editor_df

    sorted_df = editor_df.copy()
    ascending = sort_direction == "Ascending"
    temp_col = "__sort_key__"

    if sort_by in ["Appointment Date", "Reminder Date", "Critical Due Date"]:
        sorted_df[temp_col] = sorted_df[sort_by].apply(lambda v: parse_date_safe(normalize_editor_date_value(v)) or date.max)
        sorted_df = sorted_df.sort_values(by=[temp_col, "ID"], ascending=[ascending, True]).drop(columns=[temp_col])
    elif sort_by == "ID":
        sorted_df[temp_col] = sorted_df[sort_by].apply(lambda v: int(v) if str(v).strip().isdigit() else 0)
        sorted_df = sorted_df.sort_values(by=[temp_col], ascending=ascending).drop(columns=[temp_col])
    else:
        sorted_df[temp_col] = sorted_df[sort_by].fillna("").astype(str).str.lower()
        sorted_df = sorted_df.sort_values(by=[temp_col, "ID"], ascending=[ascending, True]).drop(columns=[temp_col])

    return sorted_df.reset_index(drop=True)

def compute_fast_reschedule_changes(original_df, edited_df):
    if original_df is None or edited_df is None or original_df.empty or edited_df.empty:
        return []

    original_by_id = {int(row["ID"]): row for _, row in original_df.iterrows()}
    changes = []
    for _, row in edited_df.iterrows():
        item_id = int(row["ID"])
        orig = original_by_id.get(item_id)
        if orig is None:
            continue

        original_title = str(orig.get("Title", "") or "")
        edited_title = str(row.get("Title", "") or "")
        original_dates = [
            format_date_safe(normalize_editor_date_value(orig[col]))
            for col in ["Appointment Date", "Reminder Date", "Critical Due Date"]
        ]
        edited_dates = [
            format_date_safe(normalize_editor_date_value(row[col]))
            for col in ["Appointment Date", "Reminder Date", "Critical Due Date"]
        ]
        original_flags = [bool_from_db(orig.get("Appointment", False)), bool_from_db(orig.get("Reminder", False))]
        edited_flags = [bool_from_db(row.get("Appointment", False)), bool_from_db(row.get("Reminder", False))]
        mark_complete_flag = bool_from_db(row.get("Complete", False))
        delete_flag = bool_from_db(row.get("Delete", False))
        if original_dates != edited_dates or original_title != edited_title or original_flags != edited_flags or mark_complete_flag or delete_flag:
            changes.append({
                "id": item_id,
                "title": edited_title,
                "is_appointment": int(edited_flags[0]),
                "appointment_date": edited_dates[0] if edited_flags[0] else "",
                "is_reminder": int(edited_flags[1]),
                "reminder_date": edited_dates[1] if edited_flags[1] else "",
                "critical_due_date": edited_dates[2],
                "complete": int(mark_complete_flag),
                "delete": int(delete_flag),
            })
    return changes


def save_dates_batch(changes):
    if not changes:
        return 0
    saved_count = 0
    with conn() as c:
        cur = c.cursor()
        for change in changes:
            item_id = change["id"]
            if change.get("delete", 0):
                cur.execute("UPDATE items SET project_id=NULL WHERE project_id=?", (item_id,))
                cur.execute("DELETE FROM items WHERE id=?", (item_id,))
                saved_count += 1
                continue
            if change.get("complete", 0):
                cur.execute("UPDATE items SET state='Complete', in_today=0 WHERE id=?", (item_id,))
                saved_count += 1
                continue
            cur.execute(
                """
                UPDATE items
                SET title=?, is_appointment=?, appointment_date=?, is_reminder=?, reminder_date=?, critical_due_date=?
                WHERE id=?
                """,
                (change["title"], change.get("is_appointment", 0), change.get("appointment_date", ""), change.get("is_reminder", 0), change.get("reminder_date", ""), change.get("critical_due_date", ""), item_id)
            )
            saved_count += 1
        c.commit()
    invalidate_schedule_caches()
    return saved_count


def apply_fast_reschedule_filters(
    df_items,
    search_text,
    subject,
    work_type,
    priority_type,
    item_type,
    project_name,
    open_only,
    row_limit,
):
    df = df_items.copy()
    if search_text.strip():
        df = search_items_df(df, search_text)
    if subject != "All":
        df = df[df["subject"] == subject]
    if work_type != "All":
        df = df[df["work_type"] == work_type]
    if priority_type != "All":
        df = df[df["priority_type"].fillna("Task Repository") == priority_type]
    if project_name != "All":
        proj_reverse = {v: k for k, v in project_name_map().items()}
        selected_project_id = proj_reverse.get(project_name)
        df = df[df["project_id"] == selected_project_id]
    if open_only:
        df = df[df["state"].fillna("Open") != "Complete"]
    df = df.sort_values(by=["subject", "title", "id"]).head(row_limit)
    return df



def build_items_report_pdf(report_df, report_title="Items Report", filter_summary="", include_today_order=False):
    buffer = BytesIO()
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(letter),
        rightMargin=0.35 * inch,
        leftMargin=0.35 * inch,
        topMargin=0.35 * inch,
        bottomMargin=0.35 * inch,
    )
    styles = getSampleStyleSheet()
    title_style = styles["Title"]
    normal_style = styles["BodyText"]
    normal_style.fontSize = 7
    normal_style.leading = 9
    header_style = styles["BodyText"]
    header_style.fontSize = 7
    header_style.leading = 9
    section_style = styles["Heading2"]

    story = []
    story.append(Paragraph(pdf_escape(report_title), title_style))
    if filter_summary:
        story.append(Spacer(1, 0.08 * inch))
        story.append(Paragraph(pdf_escape(filter_summary), styles["Normal"]))
    story.append(Spacer(1, 0.10 * inch))
    story.append(Paragraph(f"Rows printed: {0 if report_df is None else len(report_df)}", styles["Normal"]))
    story.append(Spacer(1, 0.12 * inch))

    def build_report_table(df_part):
        table_data = [[
            Paragraph("<b>Item #</b>", header_style),
            Paragraph("<b>Title</b>", header_style),
            Paragraph("<b>Date Entered</b>", header_style),
            Paragraph("<b>Work Type</b>", header_style),
            *([Paragraph("<b>Today Order</b>", header_style)] if include_today_order else []),
            Paragraph("<b>Priority</b>", header_style),
            Paragraph("<b>Subject</b>", header_style),
            Paragraph("<b>Critical Due</b>", header_style),
            Paragraph("<b>Appointment</b>", header_style),
            Paragraph("<b>Reminder</b>", header_style),
        ]]

        if df_part is not None and not df_part.empty:
            for _, row in df_part.iterrows():
                appt_text = ""
                if bool_from_db(row.get("is_appointment", 0)):
                    appt_text = "Yes"
                    if format_date_safe(row.get("appointment_date", "")):
                        appt_text += f" - {format_date_report(row.get('appointment_date', ''))}"
                reminder_text = ""
                if bool_from_db(row.get("is_reminder", 0)):
                    reminder_text = "Yes"
                    if format_date_safe(row.get("reminder_date", "")):
                        reminder_text += f" - {format_date_report(row.get('reminder_date', ''))}"
                table_data.append([
                    Paragraph(pdf_escape(row.get("id", "")), normal_style),
                    Paragraph(pdf_escape(row.get("title", "")), normal_style),
                    Paragraph(pdf_escape(format_date_report(row.get("created", ""))), normal_style),
                    Paragraph(pdf_escape(row.get("work_type", "")), normal_style),
                    *([Paragraph(pdf_escape(row.get("today_execution_order", "Last") or "Last"), normal_style)] if include_today_order else []),
                    Paragraph(pdf_escape(row.get("priority_type", "")), normal_style),
                    Paragraph(pdf_escape(row.get("subject", "")), normal_style),
                    Paragraph(pdf_escape(format_date_report(row.get("critical_due_date", ""))), normal_style),
                    Paragraph(pdf_escape(appt_text), normal_style),
                    Paragraph(pdf_escape(reminder_text), normal_style),
                ])

        table = Table(
            table_data,
            colWidths=([0.45 * inch, 2.35 * inch, 0.65 * inch, 0.85 * inch] + ([0.68 * inch] if include_today_order else []) + [0.75 * inch, 1.15 * inch, 0.65 * inch, 0.78 * inch, 0.78 * inch]),
            repeatRows=1,
        )
        table.setStyle(TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightgrey),
            ("GRID", (0, 0), (-1, -1), 0.35, colors.grey),
            ("VALIGN", (0, 0), (-1, -1), "TOP"),
            ("LEFTPADDING", (0, 0), (-1, -1), 3),
            ("RIGHTPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
        ]))
        return table

    if include_today_order and report_df is not None and not report_df.empty and "today_execution_order" in report_df.columns and (report_df["priority_type"].fillna("").astype(str) == "Today").any():
        for section_name in TODAY_EXECUTION_ORDERS:
            section_df = report_df[report_df["today_execution_order"].fillna("Last") == section_name].copy()
            if section_df.empty:
                continue
            story.append(Paragraph(section_name.upper(), section_style))
            story.append(Spacer(1, 0.06 * inch))
            story.append(build_report_table(section_df))
            story.append(Spacer(1, 0.16 * inch))
    else:
        story.append(build_report_table(report_df))

    doc.build(story)
    pdf_bytes = buffer.getvalue()
    buffer.close()
    return pdf_bytes

st.title("Task Management System")
st.caption("Database Mode: CLOUD / Neon")

rt1, rt2 = st.columns([1.6, 6])
with rt1:
    if st.button("Refresh From Database"):
        invalidate_caches()
        try:
            invalidate_schedule_caches()
        except Exception:
            pass
        st.rerun()
with rt2:
    st.caption("Use this after saving changes in the Scheduling Console to reload the latest database values.")

# If a delete just happened, do NOT continue rendering the whole app.
# This is the main cloud-speed fix: the database delete is quick, but redrawing
# the full All Items page can take a long time.
if st.session_state.pop("_stop_after_delete", False):
    deleted_id = st.session_state.pop("_fast_delete_done_id", None)
    st.success(f"Deleted item #{deleted_id}.")
    c1, c2 = st.columns([1, 4])
    with c1:
        if st.button("Continue"):
            st.rerun()
    st.stop()

st.subheader("Quick Add")
if "pending_add_item" not in st.session_state:
    st.session_state.pending_add_item = None

cqa1, cqa2 = st.columns([8, 3])
with cqa1:
    txt = st.text_area(
        "Enter item",
        key=f"quick_item_text_{st.session_state.quick_add_reset_counter}",
        height=110,
    )
with cqa2:
    work_type_choice = st.selectbox("Work Type", get_work_types(), index=0, key=f"quick_work_type_{st.session_state.quick_add_reset_counter}")

# Simplified Quick Add: Ideas are handled on the Ideas page, and the main Enter Item box is the note/capture field.
cqa4, cqa5 = st.columns([3, 2])
with cqa4:
    subject_choice = st.selectbox("Subject", get_subjects(), index=0, key=f"quick_subject_{st.session_state.quick_add_reset_counter}")
with cqa5:
    priority_type_choice = st.selectbox("Priority Type", get_priority_types(), index=0, key=f"quick_priority_type_{st.session_state.quick_add_reset_counter}")

qa_ap1, qa_ap2, qa_ap3, qa_ap4, qa_ap5 = st.columns(5)
with qa_ap1:
    quick_is_appointment = st.checkbox("Appointment", key=f"quick_is_appointment_{st.session_state.quick_add_reset_counter}")
with qa_ap2:
    quick_appointment_date = st.date_input("Appointment date", value=date.today(), key=f"quick_appointment_date_{st.session_state.quick_add_reset_counter}", disabled=not quick_is_appointment)
with qa_ap3:
    quick_is_reminder = st.checkbox("Reminder / Note", key=f"quick_is_reminder_{st.session_state.quick_add_reset_counter}", help="Use this for quick thoughts or notes to review later. These can be filtered in Reports / Print.")
with qa_ap4:
    quick_reminder_date = st.date_input("Reminder date", value=date.today(), key=f"quick_reminder_date_{st.session_state.quick_add_reset_counter}", disabled=not quick_is_reminder)
with qa_ap5:
    quick_has_critical_due = st.checkbox("Critical Due Date", key=f"quick_has_critical_due_{st.session_state.quick_add_reset_counter}")
    quick_critical_due_date = st.date_input("Critical due", value=date.today(), key=f"quick_critical_due_date_{st.session_state.quick_add_reset_counter}", disabled=not quick_has_critical_due)

if st.button("Check / Add"):
    if txt.strip():
        st.session_state.pending_add_item = {
            "title": txt.strip(),
            "item_type": "Reminder" if quick_is_reminder else "Task",
            "is_appointment": int(bool(quick_is_appointment)),
            "appointment_date": format_date_safe(quick_appointment_date) if quick_is_appointment else "",
            "is_reminder": int(bool(quick_is_reminder)),
            "reminder_date": format_date_safe(quick_reminder_date) if quick_is_reminder else "",
            "critical_due_date": format_date_safe(quick_critical_due_date) if quick_has_critical_due else "",
            "work_type": work_type_choice,
            "subject": subject_choice,
            "priority_type": priority_type_choice,
            "notes": "",
            "project_id": None,
        }

pending = st.session_state.pending_add_item
if pending:
    similar_df = find_similar_items(pending["title"], limit=5)
    st.markdown("#### Duplicate Check")
    if similar_df.empty:
        st.success("No similar items found.")
    else:
        st.warning("Possible similar items found. Review before adding.")
        st.dataframe(similar_df, use_container_width=True, hide_index=True)

    ac1, ac2 = st.columns(2)
    with ac1:
        if st.button("Add Anyway"):
            add_item(
                pending["title"],
                item_type=pending["item_type"],
                work_type=pending["work_type"],
                subject=pending["subject"],
                priority_type=pending["priority_type"],
                notes=pending["notes"],
                project_id=pending["project_id"],
                is_appointment=pending.get("is_appointment", 0),
                appointment_date=pending.get("appointment_date", ""),
                is_reminder=pending.get("is_reminder", 0),
                reminder_date=pending.get("reminder_date", ""),
                critical_due_date=pending.get("critical_due_date", ""),
            )
            reset_quick_add_form()
            st.rerun()
    with ac2:
        if st.button("Cancel Add"):
            st.session_state.pending_add_item = None
            st.rerun()



st.markdown("---")

def render_iphone_today_view():
    st.markdown(
        """
        <style>
        /* iPhone Today page - phone-first layout */
        .iphone-page-title {
            font-size: 2.1rem;
            font-weight: 800;
            margin-top: 0.25rem;
            margin-bottom: 0.15rem;
        }
        .iphone-subtitle {
            color: #6b7280;
            font-size: 1.0rem;
            margin-bottom: 0.65rem;
        }
        .iphone-section-title {
            font-size: 2.0rem;
            font-weight: 900;
            letter-spacing: 0.02em;
            margin-top: 1.0rem;
            margin-bottom: 0.45rem;
            border-bottom: 2px solid #e5e7eb;
            padding-bottom: 0.25rem;
        }
        .iphone-card {
            border: 1px solid #e5e7eb;
            border-radius: 18px;
            padding: 0.9rem 0.9rem 0.75rem 0.9rem;
            margin-bottom: 0.75rem;
            box-shadow: 0 1px 4px rgba(0,0,0,0.07);
            background: white;
        }
        .iphone-title {
            font-size: 1.25rem;
            line-height: 1.35;
            font-weight: 800;
            margin-bottom: 0.3rem;
        }
        .iphone-meta {
            color: #6b7280;
            font-size: 0.92rem;
            line-height: 1.25;
            margin-bottom: 0.45rem;
        }
        .iphone-empty {
            color: #6b7280;
            font-size: 1.05rem;
            padding: 0.75rem 0.25rem 1.0rem 0.25rem;
        }
        .iphone-count {
            color: #6b7280;
            font-size: 0.95rem;
            margin-bottom: 0.5rem;
        }
        div[data-testid="stExpander"] {
            border-radius: 14px !important;
            margin-bottom: 0.25rem !important;
        }
        div.stButton > button {
            min-height: 2.7rem;
            border-radius: 12px;
            font-weight: 700;
        }
        @media (max-width: 700px) {
            .block-container {
                padding-left: 0.65rem !important;
                padding-right: 0.65rem !important;
                padding-top: 0.6rem !important;
            }
            .iphone-page-title { font-size: 1.75rem; }
            .iphone-section-title { font-size: 1.65rem; }
            .iphone-title { font-size: 1.18rem; }
            .iphone-card { padding: 0.75rem; margin-bottom: 0.6rem; }
            div.stButton > button { width: 100%; min-height: 3.0rem; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

    st.markdown('<div class="iphone-page-title">iPhone Today</div>', unsafe_allow_html=True)
    st.markdown('<div class="iphone-subtitle">Today items grouped by First, Second, and Last. Shows anything marked Priority = Today or manually added to Today.</div>', unsafe_allow_html=True)

    df_today = load_today()
    if df_today.empty:
        st.info("No items are currently marked Today or manually added to Today.")
        return

    st.markdown(f'<div class="iphone-count">Today items shown: {len(df_today)}</div>', unsafe_allow_html=True)

    def render_mobile_card(row, section_name):
        item_id = int(row.get("id"))
        title = str(row.get("title", "") or "(Untitled Item)")
        work_type = str(row.get("work_type", "") or "")
        subject = str(row.get("subject", "") or "")
        notes = str(row.get("notes", "") or "")
        priority = str(row.get("priority_type", "") or "Today")
        status_bits = appointment_reminder_status(row)
        meta_parts = []
        if work_type:
            meta_parts.append(work_type)
        if subject and subject != "Unassigned":
            meta_parts.append(f"Subject: {subject}")
        if priority and priority != "Today":
            meta_parts.append(priority)
        meta_parts.extend(status_bits)
        meta_text = " | ".join(meta_parts)

        st.markdown('<div class="iphone-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="iphone-title">#{item_id} — {pdf_escape(title)}</div>', unsafe_allow_html=True)
        if meta_text:
            st.markdown(f'<div class="iphone-meta">{pdf_escape(meta_text)}</div>', unsafe_allow_html=True)
        if notes.strip():
            preview = notes.strip()
            if len(preview) > 130:
                preview = preview[:127] + "..."
            st.caption(f"Notes: {preview}")

        done_col, edit_col = st.columns([1.2, 2.0])
        with done_col:
            if st.button("✅ Done", key=f"iphone_done_{item_id}"):
                mark_complete(item_id)
                st.rerun()
        with edit_col:
            with st.expander("Edit / Update"):
                new_title = st.text_area(
                    "Title",
                    value=title,
                    height=120,
                    key=f"iphone_title_{item_id}",
                )
                new_notes = st.text_area(
                    "Notes",
                    value=notes,
                    height=90,
                    key=f"iphone_notes_{item_id}",
                )
                order_choices = TODAY_EXECUTION_ORDERS
                current_order = str(row.get("today_execution_order", "Last") or "Last")
                if current_order not in order_choices:
                    current_order = "Last"
                new_order = st.selectbox(
                    "Today Order",
                    order_choices,
                    index=order_choices.index(current_order),
                    key=f"iphone_order_{item_id}",
                )
                action_col1, action_col2 = st.columns(2)
                with action_col1:
                    if st.button("Save Update", key=f"iphone_save_{item_id}"):
                        save_item(
                            item_id,
                            new_title,
                            row.get("item_type", "Task") or "Task",
                            row.get("work_type", "Unassigned") or "Unassigned",
                            row.get("subject", "Unassigned") or "Unassigned",
                            row.get("priority_type", "Today") or "Today",
                            new_notes,
                            row.get("project_id", None),
                            row.get("due", "") or "",
                            row.get("review_date", "") or "",
                            row.get("review_start", "") or "",
                            row.get("review_end", "") or "",
                            is_appointment=row.get("is_appointment", None),
                            appointment_date=row.get("appointment_date", None),
                            is_reminder=row.get("is_reminder", None),
                            reminder_date=row.get("reminder_date", None),
                            critical_due_date=row.get("critical_due_date", None),
                            assignee_id=row.get("assignee_id", None),
                        )
                        if new_order != current_order:
                            save_today_execution_order(item_id, new_order)
                        st.rerun()
                with action_col2:
                    if st.button("Remove Today", key=f"iphone_remove_{item_id}"):
                        remove_item(item_id)
                        st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

    def render_section(section_name, expanded=True):
        section_df = df_today[df_today["today_execution_order"].fillna("Last") == section_name].copy()
        if expanded:
            st.markdown(f'<div class="iphone-section-title">{section_name.upper()}</div>', unsafe_allow_html=True)
            if section_df.empty:
                st.markdown('<div class="iphone-empty">No items.</div>', unsafe_allow_html=True)
            else:
                for _, row in section_df.iterrows():
                    render_mobile_card(row, section_name)
        else:
            with st.expander(f"{section_name.upper()}  ({len(section_df)})", expanded=False):
                if section_df.empty:
                    st.markdown('<div class="iphone-empty">No items.</div>', unsafe_allow_html=True)
                else:
                    for _, row in section_df.iterrows():
                        render_mobile_card(row, section_name)

    render_section("First", expanded=True)
    render_section("Second", expanded=False)
    render_section("Last", expanded=False)


# V20: compact wrapped tab navigation using Streamlit buttons, not links.
# This keeps navigation inside the same app/browser tab.
MAIN_TABS = [
    "iPhone Today", "Today List", "Reports / Print", "Weekly Planning",
    "All Items", "Assign Tasks", "Ideas", "Ideas / Concepts",
    "Fast Reschedule", "Subject Manager", "Work Type Manager", "Priority Manager",
    "Assignee Manager", "Completed Items", "Driving List",
]

if "active_main_tab" not in st.session_state or st.session_state.active_main_tab not in MAIN_TABS:
    st.session_state.active_main_tab = "iPhone Today"

st.markdown("""
<style>
/* V20 compact wrapped Streamlit tab buttons. No HTML links, so no new browser tabs. */
div[data-testid="stHorizontalBlock"] div.stButton > button {
    min-height: 30px !important;
    height: 32px !important;
    padding: 2px 8px !important;
    font-size: 14px !important;
    line-height: 16px !important;
    border-radius: 16px !important;
    margin: 0 !important;
    white-space: nowrap !important;
}
.compact-tab-spacer {
    margin-top: 4px;
    margin-bottom: 4px;
}
@media (max-width: 700px) {
    div[data-testid="stHorizontalBlock"] div.stButton > button {
        min-height: 28px !important;
        height: 30px !important;
        padding: 2px 6px !important;
        font-size: 12px !important;
        border-radius: 14px !important;
    }
}
</style>
""", unsafe_allow_html=True)

def _set_main_tab(tab_name):
    st.session_state.active_main_tab = tab_name

# Wrapped rows. Streamlit buttons change session state in-place and do not open browser tabs.
_tab_rows = [MAIN_TABS[0:5], MAIN_TABS[5:10], MAIN_TABS[10:15]]
for _row in _tab_rows:
    _cols = st.columns(len(_row), gap="small")
    for _col, _label in zip(_cols, _row):
        with _col:
            _button_type = "primary" if _label == st.session_state.active_main_tab else "secondary"
            if st.button(_label, key=f"main_nav_{_label}", type=_button_type, use_container_width=True):
                _set_main_tab(_label)
                st.rerun()
st.markdown('<div class="compact-tab-spacer"></div>', unsafe_allow_html=True)

current_page = st.session_state.active_main_tab

if current_page == "iPhone Today":
    render_iphone_today_view()


if current_page == "Reports / Print":
    st.subheader("Reports / Print")
    st.write("Create a printable item report by Work Type, Priority, Subject, Reminder status, Critical Due Date, and Date Entered.")
    st.caption("The report now runs only when you press Run / Refresh Report. This prevents large database queries and PDF creation during normal startup or page navigation.")

    with st.form("reports_filter_form_v27_3", clear_on_submit=False):
        rf1, rf2, rf3, rf4, rf5 = st.columns(5)
        with rf1:
            report_work_type = st.selectbox("Work Type", ["All"] + get_work_types(), index=0, key="report_work_type_filter_v273")
        with rf2:
            report_priority = st.selectbox("Priority", ["All"] + get_priority_types(), index=0, key="report_priority_filter_v273")
        with rf3:
            report_subject = st.selectbox("Subject", ["All"] + get_subjects(), index=0, key="report_subject_filter_v273")
        with rf4:
            report_reminder_mode = st.selectbox("Reminder", ["All", "Only Reminders", "Exclude Reminders"], index=0, key="report_reminder_mode_v273")
        with rf5:
            critical_due_mode = st.selectbox("Critical Due Date", ["All", "Specific Date", "Only Items With Critical Due Date"], index=0, key="report_critical_due_mode_v273")
            critical_due_filter_date = st.date_input("Choose critical due date", value=date.today(), key="report_critical_due_date_v273", disabled=(critical_due_mode != "Specific Date"))

        st.markdown("**Date Entered Filter**")
        df1, df2, df3, df4 = st.columns([1, 1, 1, 1])
        with df1:
            date_entered_start_mode = st.selectbox("Start Date", ["None", "Use Start Date"], index=0, key="report_created_start_mode_v273")
        with df2:
            date_entered_start_date = st.date_input("Choose start date", value=date.today(), key="report_created_start_date_v273", disabled=(date_entered_start_mode == "None"))
        with df3:
            date_entered_end_mode = st.selectbox("End Date", ["None", "Use End Date"], index=0, key="report_created_end_mode_v273")
        with df4:
            date_entered_end_date = st.date_input("Choose end date", value=date.today(), key="report_created_end_date_v273", disabled=(date_entered_end_mode == "None"))

        sf1, sf2 = st.columns(2)
        with sf1:
            report_sort_by = st.selectbox(
                "Sort By",
                ["Date Entered", "Subject", "Item #", "Work Type", "Priority", "Critical Due Date"],
                index=0,
                key="report_sort_by_v273",
            )
        with sf2:
            report_sort_direction = st.selectbox("Sort Direction", ["Ascending", "Descending"], index=0, key="report_sort_direction_v273")

        run_report = st.form_submit_button("Run / Refresh Report", type="primary")

    if run_report:
        st.session_state["report_has_run_v273"] = True
        st.session_state.pop("report_pdf_v273", None)

    if st.session_state.get("report_has_run_v273", False):
        with st.spinner("Loading report data..."):
            report_df = load_all().copy()

        if report_df.empty:
            st.info("No open items available for reporting.")
        else:
            if report_work_type != "All":
                report_df = report_df[report_df["work_type"].fillna("").astype(str).str.strip() == str(report_work_type).strip()]
            if report_priority != "All":
                report_df = report_df[report_df["priority_type"].fillna("Task Repository").astype(str).str.strip() == str(report_priority).strip()]
            if report_subject != "All":
                report_df = report_df[report_df["subject"].fillna("Unassigned").astype(str).str.strip() == str(report_subject).strip()]
            if report_reminder_mode == "Only Reminders":
                report_df = report_df[report_df["is_reminder"].apply(bool_from_db)].copy()
            elif report_reminder_mode == "Exclude Reminders":
                report_df = report_df[~report_df["is_reminder"].apply(bool_from_db)].copy()
            if critical_due_mode == "Specific Date":
                selected_critical = format_date_safe(critical_due_filter_date)
                report_df = report_df[report_df["critical_due_date"].apply(format_date_safe) == selected_critical]
            elif critical_due_mode == "Only Items With Critical Due Date":
                report_df = report_df[report_df["critical_due_date"].apply(lambda x: bool(format_date_safe(x)))]

            report_df["created_filter_date"] = report_df["created"].apply(parse_date_safe)
            if date_entered_start_mode == "Use Start Date":
                report_df = report_df[report_df["created_filter_date"].apply(lambda d: d is not None and d >= date_entered_start_date)]
            if date_entered_end_mode == "Use End Date":
                report_df = report_df[report_df["created_filter_date"].apply(lambda d: d is not None and d <= date_entered_end_date)]

            if "today_execution_order" not in report_df.columns:
                report_df["today_execution_order"] = "Last"
            report_df["today_execution_order"] = report_df["today_execution_order"].fillna("Last").replace("", "Last")
            report_df["today_order_sort"] = report_df["today_execution_order"].map({"First": 1, "Second": 2, "Last": 3}).fillna(3)
            report_df["created_sort"] = report_df["created"].apply(report_sort_date)
            report_df["critical_due_sort"] = report_df["critical_due_date"].apply(report_sort_date) if "critical_due_date" in report_df.columns else date(1900, 1, 1)

            sort_map = {
                "Item #": "id",
                "Date Entered": "created_sort",
                "Work Type": "work_type",
                "Subject": "subject",
                "Priority": "priority_type",
                "Critical Due Date": "critical_due_sort",
            }
            primary_sort_col = sort_map.get(report_sort_by, "created_sort")
            include_today_order_report = (report_priority == "Today")
            sort_cols = ["today_order_sort"] if include_today_order_report else []
            if primary_sort_col not in sort_cols:
                sort_cols.append(primary_sort_col)
            for extra_col in ["subject", "work_type", "priority_type", "created_sort", "critical_due_sort", "title", "id"]:
                if extra_col not in sort_cols and extra_col in report_df.columns:
                    sort_cols.append(extra_col)
            report_df = report_df.sort_values(by=sort_cols, ascending=(report_sort_direction == "Ascending"))

            report_display_df = report_df.copy()
            for dc in ["created", "critical_due_date", "appointment_date", "reminder_date"]:
                if dc in report_display_df.columns:
                    report_display_df[dc] = report_display_df[dc].apply(format_date_report)
            display_base = ["id", "title", "created", "work_type"]
            if include_today_order_report:
                display_base.append("today_execution_order")
            display_base += ["priority_type", "subject", "critical_due_date", "is_appointment", "appointment_date", "is_reminder", "reminder_date"]
            display_cols = [c for c in display_base if c in report_display_df.columns]
            st.caption(f"Report rows: {len(report_df)}")
            st.dataframe(report_display_df[display_cols], use_container_width=True, hide_index=True)

            date_entered_summary = "All"
            if date_entered_start_mode == "Use Start Date" and date_entered_end_mode == "Use End Date":
                date_entered_summary = f"{format_date_report(date_entered_start_date)} to {format_date_report(date_entered_end_date)}"
            elif date_entered_start_mode == "Use Start Date":
                date_entered_summary = f"From {format_date_report(date_entered_start_date)} forward"
            elif date_entered_end_mode == "Use End Date":
                date_entered_summary = f"Through {format_date_report(date_entered_end_date)}"

            filter_summary = (
                f"Work Type: {report_work_type} | Priority: {report_priority} | Subject: {report_subject} | Reminder: {report_reminder_mode} | Critical Due Date: "
                + (format_date_safe(critical_due_filter_date) if critical_due_mode == "Specific Date" else critical_due_mode)
                + f" | Date Entered: {date_entered_summary}"
                + f" | Sort By: {report_sort_by} ({report_sort_direction})"
            )

            if st.button("Prepare PDF", key="prepare_report_pdf_v273"):
                with st.spinner("Preparing PDF..."):
                    st.session_state["report_pdf_v273"] = build_items_report_pdf(
                        report_df,
                        report_title="Task Flow Items Report",
                        filter_summary=filter_summary,
                        include_today_order=include_today_order_report,
                    )

            if st.session_state.get("report_pdf_v273"):
                st.download_button(
                    "Download / Print Filtered Report PDF",
                    data=st.session_state["report_pdf_v273"],
                    file_name="task_flow_filtered_report.pdf",
                    mime="application/pdf",
                    key="download_filtered_report_pdf_v273",
                )

if current_page == "Today List":
    st.subheader("Today List")
    df_today = load_today()
    if df_today.empty:
        st.info("No items in Today List")
    else:
        for today_order in TODAY_EXECUTION_ORDERS:
            section_df = df_today[df_today["today_execution_order"].fillna("Last") == today_order].copy()
            if section_df.empty:
                continue
            st.markdown(f"# {today_order.upper()}")
            for g, group in section_df.groupby("work_type"):
                st.markdown(f"## {g}")
                for r in group.itertuples():
                    project_note = ""
                    if pd.notna(r.project_id) and r.project_id:
                        proj_df = pd.read_sql("SELECT title FROM items WHERE id=?", conn(), params=(int(r.project_id),))
                        if not proj_df.empty:
                            project_note = f"Idea: {proj_df.iloc[0]['title']}"
                    status_bits = appointment_reminder_status(r)
                    if r.item_type in ("Project", "Ideas"):
                        status_bits.append("IDEA REVIEW / ADVANCEMENT")
                    if r.subject and str(r.subject).strip() and r.subject != "Unassigned":
                        status_bits.append(f"Subject: {r.subject}")
                    if getattr(r, "priority_type", "Task Repository") == "Scan / File":
                        status_bits.append("SCAN / FILE")
                    status_bits.append(f"Today Order: {getattr(r, 'today_execution_order', 'Last') or 'Last'}")
                    detail_line = " | ".join([x for x in [project_note] + status_bits if x])
                    col1, col2, col3, col4 = st.columns([6, 1.4, 1, 1])
                    col1.write(f"- **#{r.id}** {r.title}  \n  *{detail_line}*")
                    current_order = getattr(r, "today_execution_order", "Last") or "Last"
                    if current_order not in TODAY_EXECUTION_ORDERS:
                        current_order = "Last"
                    new_today_order = col2.selectbox(
                        "Order",
                        TODAY_EXECUTION_ORDERS,
                        index=TODAY_EXECUTION_ORDERS.index(current_order),
                        key=f"today_order_{r.id}",
                    )
                    if new_today_order != current_order:
                        save_today_execution_order(r.id, new_today_order)
                        st.rerun()
                    if col3.button("Remove", key=f"rem_{r.id}"):
                        remove_item(r.id)
                        st.rerun()
                    if col4.button("Complete", key=f"complete_today_{r.id}"):
                        mark_complete(r.id)
                        st.rerun()


if current_page == "Weekly Planning":
    st.subheader("Weekly Planning / Queue View")
    st.caption("This view now uses smarter scoring and balance rules to surface a more workable mix for the week.")
    week_start, week_end = week_window()
    st.caption(f"Week: {week_start.strftime('%m/%d/%Y')} to {week_end.strftime('%m/%d/%Y')}")

    wc1, wc2 = st.columns(2)
    with wc1:
        if st.button("Add Top Weekly Items to Today"):
            added = add_top_weekly_items_to_today(max_items=5)
            if added:
                st.success(f"Added {added} weekly item(s) to Today List.")
            else:
                st.info("No weekly items available to add.")
            st.rerun()
    with wc2:
        weekly_df = load_weekly_queue()
        st.metric("Weekly Queue Items", 0 if weekly_df.empty else len(weekly_df))

    weekly_df = load_weekly_queue()

    if weekly_df.empty:
        st.info("No items currently flagged for this week.")
    else:
        if "subject" in weekly_df.columns:
            weekly_subjects = ["All"] + sorted([s for s in weekly_df["subject"].dropna().unique().tolist() if str(s).strip()], key=str.lower)
            selected_weekly_subject = st.selectbox("Filter weekly queue by Subject", weekly_subjects, index=0, key="weekly_subject_filter")
            if selected_weekly_subject != "All":
                weekly_df = weekly_df[weekly_df["subject"] == selected_weekly_subject]

        # Bucket items into practical weekly buckets
        urgent_rows = []
        due_this_week_rows = []
        review_rows = []
        project_rows = []

        for _, row in weekly_df.iterrows():
            flags = weekly_status(row)
            item_type = row.get("item_type", "")

            if "OVERDUE" in flags or "REVIEW DUE" in flags or "WINDOW OVERDUE" in flags:
                urgent_rows.append(row)
            elif "DUE THIS WEEK" in flags:
                due_this_week_rows.append(row)
            elif "REVIEW THIS WEEK" in flags or "ACTIVE WINDOW" in flags or "WINDOW STARTS THIS WEEK" in flags:
                review_rows.append(row)
            elif item_type in ("Project", "Ideas"):
                project_rows.append(row)
            else:
                due_this_week_rows.append(row)

        def render_week_bucket(title, rows, prefix):
            st.markdown(f"### {title}")
            if not rows:
                st.caption("None")
                return
            for idx, row in enumerate(rows):
                status_bits = weekly_status(row)
                subject_text = row.get("subject", "")
                if subject_text and str(subject_text).strip() and subject_text != "Unassigned":
                    status_bits.append(f"Subject: {subject_text}")
                if row.get("item_type") in ("Project", "Ideas"):
                    status_bits.append("Idea Work")

                detail = " | ".join(status_bits)
                c1, c2 = st.columns([8, 1])
                c1.write(f"- {row['title']}  \n  *{row.get('work_type','')}" + (f" | {detail}" if detail else "") + "*")
                if c2.button("Add to Today", key=f"{prefix}_{int(row['id'])}_{idx}"):
                    add_to_today(int(row["id"]))
                    st.rerun()

        render_week_bucket("Urgent / Overdue", urgent_rows, "wk_urgent")
        render_week_bucket("Due This Week", due_this_week_rows, "wk_due")
        render_week_bucket("Review / Ideas Attention This Week", review_rows + project_rows, "wk_review")
        backlog_only = [r for r in weekly_df.to_dict("records") if r.get("priority_type", "Task Repository") == "Scan / File"]
        render_week_bucket("Scan / File / Fill Work", backlog_only, "wk_scan_file")


if current_page == "All Items":
    st.subheader("All Items")
    df_all = load_all()
    if df_all.empty:
        st.info("No items yet")
    else:
        st.markdown("### Search Items")
        search_query = st.text_input("Search for matching words in items", key="all_items_search_query")
        search_results = search_items_df(df_all, search_query)

        if search_query.strip():
            st.caption(f"Matches found: {len(search_results)}")
            if not search_results.empty:
                if "all_items_search_index" not in st.session_state:
                    st.session_state.all_items_search_index = 0

                max_index = len(search_results) - 1
                if st.session_state.all_items_search_index > max_index:
                    st.session_state.all_items_search_index = 0

                nav1, nav2, nav3 = st.columns([1, 1, 6])
                with nav1:
                    if st.button("Previous", key="all_items_search_prev"):
                        st.session_state.all_items_search_index = max(0, st.session_state.all_items_search_index - 1)
                with nav2:
                    if st.button("Next", key="all_items_search_next"):
                        st.session_state.all_items_search_index = min(max_index, st.session_state.all_items_search_index + 1)
                with nav3:
                    st.caption(f"Viewing match {st.session_state.all_items_search_index + 1} of {len(search_results)}")

                current_match = search_results.iloc[st.session_state.all_items_search_index]
                current_title = str(current_match.get("title", "") or "")
                current_subject = str(current_match.get("subject", "") or "")
                current_work_type = str(current_match.get("work_type", "") or "")
                current_notes = str(current_match.get("notes", "") or "")
                st.info(
                    f"Current Match: {current_title}"
                    + (f" | Subject: {current_subject}" if current_subject and current_subject != "Unassigned" else "")
                    + (f" | Work Type: {current_work_type}" if current_work_type else "")
                )
                if current_notes.strip():
                    st.caption(f"Notes: {current_notes}")

                display_cols = [c for c in ["id","title","work_type","subject","priority_type","critical_due_date"] if c in search_results.columns]
                st.dataframe(search_results[display_cols], use_container_width=True, hide_index=True)
            else:
                st.info("No matching items found.")

        st.markdown("---")
        filter_work_type = st.selectbox("Filter by Work Type", ["All"] + get_work_types(), index=0, key="all_filter_work")
        filter_subject = st.selectbox("Filter by Subject", ["All"] + get_subjects(), index=0, key="all_filter_subject")
        filter_priority = st.selectbox("Filter by Priority", ["All"] + get_priority_types(), index=0, key="all_filter_priority")
        all_items_sort_order = st.selectbox(
            "Sort All Items by Item #",
            ["Newest First (highest item # first)", "Oldest First (lowest item # first)"],
            index=0,
            key="all_items_sort_order",
        )

        all_items_row_limit = st.selectbox(
            "Rows to load/edit on this page",
            [10, 25, 50, 100, 250, "All"],
            index=1,
            key="all_items_row_limit",
            help="Cloud mode is much faster when this page does not render every editable item card at once.",
        )

        if filter_work_type != "All":
            df_all = df_all[df_all["work_type"] == filter_work_type]
        if filter_subject != "All":
            df_all = df_all[df_all["subject"] == filter_subject]
        if filter_priority != "All":
            df_all = df_all[df_all["priority_type"].fillna("Task Repository") == filter_priority]

        if all_items_sort_order == "Oldest First (lowest item # first)":
            df_all = df_all.sort_values(by="id", ascending=True)
        else:
            df_all = df_all.sort_values(by="id", ascending=False)

        total_after_filters = len(df_all)
        if all_items_row_limit != "All":
            df_all = df_all.head(int(all_items_row_limit)).copy()
        st.caption(f"Showing {len(df_all)} of {total_after_filters} matching item(s). Use filters or search to avoid loading every card.")

        current_proj_map = project_name_map()
        current_proj_reverse = {v: k for k, v in current_proj_map.items()}
        current_proj_values = list(current_proj_map.values())
        work_type_choices_all = get_work_types()
        subject_choices_all = get_subjects()
        priority_choices_all = get_priority_types()
        assignee_options_all = get_assignee_options(include_unassigned=True)

        for r in df_all.itertuples():
            with st.container(border=True):
                project_label = current_proj_map.get(int(r.project_id), "None") if pd.notna(r.project_id) and r.project_id else "None"
                subject_text = r.subject if pd.notna(r.subject) and str(r.subject).strip() else "Unassigned"
                priority_text = r.priority_type if pd.notna(r.priority_type) and str(r.priority_type).strip() else "Task Repository"
                status_bits = appointment_reminder_status(r)
                new_title = st.text_area(
                    f"Title #{r.id}",
                    value=r.title if r.title else "",
                    key=f"title_{r.id}",
                    height=title_text_area_height(r.title),
                )
                st.write("")
                st.caption(
                    f"State: {r.state if pd.notna(r.state) and str(r.state).strip() else 'Open'} | Work Type: {r.work_type} | Subject: {subject_text} | Priority: {priority_text} | Assignee: {assignee_id_to_name(getattr(r, 'assignee_id', None))} | Idea: {project_label}"
                    + (f" | {' | '.join(status_bits)}" if status_bits else "")
                )

                ec1, ec2, ec3, ec4 = st.columns(4)
                new_item_type = r.item_type if str(r.item_type or "").strip() else "Task"
                with ec1:
                    current_work_type = r.work_type if r.work_type in work_type_choices_all else "Unassigned"
                    new_work_type = st.selectbox(f"Work Type #{r.id}", work_type_choices_all, index=work_type_choices_all.index(current_work_type), key=f"wt_{r.id}")
                with ec2:
                    current_assignee_name = assignee_id_to_name(getattr(r, "assignee_id", None))
                    if current_assignee_name not in assignee_options_all:
                        current_assignee_name = "Unassigned"
                    new_assignee_name = st.selectbox(f"Assignee #{r.id}", assignee_options_all, index=assignee_options_all.index(current_assignee_name), key=f"assignee_{r.id}")
                with ec3:
                    current_subject = r.subject if r.subject in subject_choices_all else "Unassigned"
                    new_subject = st.selectbox(f"Subject #{r.id}", subject_choices_all, index=subject_choices_all.index(current_subject), key=f"subj_{r.id}")
                with ec4:
                    current_priority = r.priority_type if r.priority_type in priority_choices_all else "Task Repository"
                    new_priority_type = st.selectbox(f"Priority #{r.id}", priority_choices_all, index=priority_choices_all.index(current_priority) if current_priority in priority_choices_all else 0, key=f"prio_{r.id}")

                notes_val = st.text_input(f"Notes #{r.id}", value=r.notes if pd.notna(r.notes) else "", key=f"notes_{r.id}")
                project_select = st.selectbox(f"Idea #{r.id}", current_proj_values, index=current_proj_values.index(project_label) if project_label in current_proj_values else 0, key=f"proj_{r.id}")
                critical_due_value = clearable_date_input(
                    f"Critical Due Date #{r.id} (MM/DD/YYYY)",
                    parse_date_safe(getattr(r, "critical_due_date", "")),
                    f"critical_due_{r.id}"
                )

                action1, action2, action3, action4 = st.columns(4)
                if action1.button("Save", key=f"save_{r.id}"):
                    chosen_project_id = current_proj_reverse.get(project_select, 0)
                    save_item(
                        r.id, new_title, new_item_type, new_work_type, new_subject, new_priority_type, notes_val,
                        None if chosen_project_id == 0 else chosen_project_id,
                        "", "", "", "",
                        critical_due_date=format_date_safe(critical_due_value),
                        assignee_id=assignee_name_to_id(new_assignee_name)
                    )
                    st.rerun()
                if action2.button("Mark Complete", key=f"mark_complete_{r.id}"):
                    mark_complete(r.id)
                    st.rerun()
                if action3.button("Reopen", key=f"reopen_{r.id}"):
                    mark_open(r.id)
                    st.rerun()
                with action4:
                    st.button(
                        "Delete Item",
                        key=f"all_items_delete_{r.id}",
                        type="primary",
                        on_click=delete_item_and_stop,
                        args=(int(r.id),),
                        help="Deletes immediately, then stops before the full page redraws.",
                    )

if current_page == "Ideas":
    st.subheader("Ideas")
    projects_df = load_projects()

    st.markdown("### Search Ideas")
    idea_search_text = st.text_input("Search Ideas by keyword", key="ideas_search_text")
    idea_subject_options = ["All"] + get_subjects()
    idea_subject_filter = st.selectbox("Search Ideas by subject", idea_subject_options, index=0, key="ideas_subject_filter")
    filtered_projects_df = projects_df.copy()
    if not filtered_projects_df.empty and idea_subject_filter != "All":
        filtered_projects_df = filtered_projects_df[filtered_projects_df["subject"].fillna("Unassigned") == idea_subject_filter].copy()
    if not filtered_projects_df.empty and idea_search_text.strip():
        idea_q = idea_search_text.strip().lower()
        filtered_projects_df = filtered_projects_df[filtered_projects_df.apply(
            lambda row: idea_q in str(row.get("title", "")).lower()
            or idea_q in str(row.get("notes", "")).lower()
            or idea_q in str(row.get("subject", "")).lower(),
            axis=1
        )].copy()

    st.markdown("### Print Ideas")
    idea_print_subject = st.selectbox("Print ideas by subject", ["All"] + get_subjects(), index=0, key="ideas_print_subject")
    ideas_print_df = projects_df.copy()
    if not ideas_print_df.empty and idea_print_subject != "All":
        ideas_print_df = ideas_print_df[ideas_print_df["subject"].fillna("Unassigned") == idea_print_subject].copy()
    ideas_print_pdf = build_ideas_print_pdf(ideas_print_df, idea_print_subject)
    st.download_button(
        "Download / Print Ideas PDF",
        data=ideas_print_pdf,
        file_name="ideas_report.pdf",
        mime="application/pdf",
        key="ideas_print_pdf",
    )

    st.markdown("---")
    st.markdown("### Create Idea")
    if "idea_create_reset_counter" not in st.session_state:
        st.session_state.idea_create_reset_counter = 0
    idea_create_key_suffix = st.session_state.idea_create_reset_counter
    cp1, cp2 = st.columns([4, 2.5])
    with cp1:
        new_project_title = st.text_input("Idea title", key=f"new_idea_title_{idea_create_key_suffix}")
    with cp2:
        new_project_subject = st.selectbox("Idea subject", get_subjects(), index=0, key=f"new_idea_subject_{idea_create_key_suffix}")
    new_project_notes = st.text_area("Idea notes / description", height=120, key=f"new_idea_notes_{idea_create_key_suffix}")
    if st.button("Create Idea"):
        if new_project_title.strip():
            add_item(
                new_project_title.strip(), item_type="Ideas", work_type="Ideas", subject=new_project_subject,
                notes=new_project_notes.strip()
            )
            st.session_state.idea_create_reset_counter += 1
            st.rerun()

    st.markdown("---")
    if filtered_projects_df.empty:
        st.info("No ideas matched your search/filter" if not projects_df.empty else "No ideas yet")
    else:
        project_options = {f"{r.id} - {r.title}": int(r.id) for r in filtered_projects_df.itertuples()}
        selected_project_label = st.selectbox("Select Idea", list(project_options.keys()))
        selected_project_id = project_options[selected_project_label]

        project_row = pd.read_sql("SELECT * FROM items WHERE id=?", conn(), params=(selected_project_id,)).iloc[0]
        st.markdown("### Idea")
        ph1, ph2 = st.columns([4, 2.5])
        with ph1:
            project_title_edit = st.text_input("Idea title", value=project_row["title"], key=f"project_header_title_{selected_project_id}")
        with ph2:
            current_project_subject = project_row["subject"] if project_row["subject"] in get_subjects() else "Unassigned"
            project_subject_edit = st.selectbox("Idea subject", get_subjects(), index=get_subjects().index(current_project_subject), key=f"project_header_subject_{selected_project_id}")
        project_description_edit = st.text_area(
            "Idea notes / description",
            value=project_row["notes"] if pd.notna(project_row["notes"]) else "",
            height=120,
            key=f"project_header_desc_{selected_project_id}"
        )
        pha1, pha2, pha3, pha4 = st.columns(4)
        if pha1.button("Save Idea", key=f"save_project_header_{selected_project_id}"):
            save_item(
                selected_project_id,
                project_title_edit,
                "Ideas",
                "Ideas",
                project_subject_edit,
                project_row["priority_type"] if pd.notna(project_row["priority_type"]) else "Task Repository",
                project_description_edit.strip(),
                None,
                "", "", "", "",
            )
            st.rerun()
        if pha2.button("Mark Idea Complete", key=f"complete_project_header_{selected_project_id}"):
            mark_complete(selected_project_id)
            st.rerun()
        if pha3.button("Reopen Idea", key=f"reopen_project_header_{selected_project_id}"):
            mark_open(selected_project_id)
            st.rerun()
        with pha4:
            with st.expander("Delete Idea", expanded=False):
                st.warning("This permanently deletes this idea header. Child items are detached and kept.")
                if st.button("Delete Idea Now", key=f"delete_idea_header_{selected_project_id}", type="primary"):
                    delete_item(selected_project_id)
                    st.rerun()

        st.markdown("#### Add item / task from this idea")
        ap1, ap3, ap5 = st.columns([7, 1.5, 2.5])
        with ap1:
            project_child_title = st.text_input("Item title", key="proj_child_title")
        project_child_item_type = "Task"
        with ap3:
            project_child_work_type = st.selectbox("Work Type", get_work_types(), index=0, key="proj_child_work_type")
        with ap5:
            default_subject = project_row["subject"] if project_row["subject"] in get_subjects() else "Unassigned"
            project_child_subject = st.selectbox("Subject", get_subjects(), index=get_subjects().index(default_subject), key="proj_child_subject")
        project_child_notes = st.text_input("Notes", key="proj_child_notes")
        if st.button("Add Item from Idea"):
            if project_child_title.strip():
                add_item(
                    project_child_title.strip(), item_type=project_child_item_type, work_type=project_child_work_type,
                    subject=project_child_subject, notes=project_child_notes.strip(), project_id=selected_project_id
                )
                st.rerun()

        st.markdown("#### Idea Items")
        children_df = load_project_children(selected_project_id)
        if children_df.empty:
            st.info("No items under this idea yet")
        else:
            for r in children_df.itertuples():
                status_bits = appointment_reminder_status(r)
                subject_part = f" | {r.subject}" if pd.notna(r.subject) and r.subject != "Unassigned" else ""
                c1, c2, c3, c4 = st.columns([7, 1, 1, 1.4])
                c1.write(f"- {r.title}  \n  *{r.work_type}{subject_part}" + (f" | {' | '.join(status_bits)}" if status_bits else "") + "*")
                if c2.button("Add to Today", key=f"child_today_{r.id}"):
                    add_to_today(r.id)
                    st.rerun()
                if c3.button("Complete", key=f"child_complete_{r.id}"):
                    mark_complete(r.id)
                    st.rerun()
                with c4:
                    with st.expander("Delete", expanded=False):
                        if st.button("Delete Now", key=f"child_delete_{r.id}", type="primary"):
                            delete_item(r.id)
                            st.rerun()

if current_page == "Ideas / Concepts":
    st.subheader("Ideas / Concepts")
    df_pc = load_projects_and_concepts()
    if df_pc.empty:
        st.info("No ideas or concepts yet")
    else:
        for item_type, group in df_pc.groupby("item_type"):
            st.markdown(f"## {item_type}s")
            for r in group.itertuples():
                status_bits = appointment_reminder_status(r)
                subject_part = f" | Subject: {r.subject}" if pd.notna(r.subject) and r.subject != "Unassigned" else ""
                cpc1, cpc2 = st.columns([8, 1])
                cpc1.write(f"- {r.title}  \n  *{r.work_type}{subject_part}" + (f" | {' | '.join(status_bits)}" if status_bits else "") + "*")
                if cpc2.button("Add to Today", key=f"pc_add_{r.id}"):
                    add_to_today(r.id)
                    st.rerun()


if current_page == "Fast Reschedule":
    
    st.markdown("""
    <style>
    [data-testid="stDataFrame"] div[role="gridcell"] {
        white-space: normal !important;
        word-break: break-word !important;
        overflow-wrap: break-word !important;
        line-height: 1.35em !important;
    }
    
    [data-testid="stDataFrame"] div[role="row"] {
        min-height: 65px !important;
    }
    </style>
    """, unsafe_allow_html=True)
    
    
    st.subheader("Fast Reschedule Grid")
    st.write("Use this page to edit titles, appointment dates, and reminder dates, then save all changes once.")

    if "fr_grid_original" not in st.session_state:
        st.session_state.fr_grid_original = None
    if "fr_grid_loaded" not in st.session_state:
        st.session_state.fr_grid_loaded = None
    if "fr_grid_meta" not in st.session_state:
        st.session_state.fr_grid_meta = ""

    fr_source_df = load_fast_reschedule_items()
    if fr_source_df.empty:
        st.info("No items yet.")
    else:
        fc1, fc2, fc3, fc4 = st.columns(4)
        with fc1:
            fr_search = st.text_input("Search title / notes / subject / work type", key="fast_reschedule_search")
        with fc2:
            fr_subject = st.selectbox("Filter by Subject", ["All"] + get_subjects(), index=0, key="fast_reschedule_subject")
        with fc3:
            fr_work_type = st.selectbox("Filter by Work Type", ["All"] + get_work_types(), index=0, key="fast_reschedule_work_type")
        with fc4:
            fr_priority = st.selectbox("Filter by Priority", ["All"] + get_priority_types(), index=0, key="fast_reschedule_priority")
        fr_item_type = "All"

        project_choices = ["All"] + [v for k, v in project_name_map().items() if k != 0]
        fd1, fd2, fd3 = st.columns(3)
        with fd1:
            fr_project = st.selectbox("Filter by Idea", project_choices, index=0, key="fast_reschedule_project")
        with fd2:
            fr_open_only = st.checkbox("Only open items", value=True, key="fast_reschedule_open_only")
        with fd3:
            fr_limit = st.selectbox("Rows to show", [25, 50, 100, 200, 500], index=2, key="fast_reschedule_limit")

        fs1, fs2 = st.columns([2, 1])
        fast_reschedule_sort_columns = ["ID", "Title", "Priority", "Subject", "Work Type", "Idea", "Appointment", "Appointment Date", "Reminder", "Reminder Date", "Critical Due Date"]
        with fs1:
            fr_sort_by = st.selectbox("Sort Fast Reschedule by", fast_reschedule_sort_columns, index=0, key="fast_reschedule_sort_by")
        with fs2:
            fr_sort_direction = st.selectbox("Sort Direction", ["Ascending", "Descending"], index=0, key="fast_reschedule_sort_direction")

        filtered_fr_df = apply_fast_reschedule_filters(
            fr_source_df,
            fr_search,
            fr_subject,
            fr_work_type,
            fr_priority,
            fr_item_type,
            fr_project,
            fr_open_only,
            fr_limit,
        )

        filter_summary = f"Current filter result: {len(filtered_fr_df)} item(s). The grid below updates automatically from your filters unless you have unsaved changes."
        st.caption(filter_summary)

        current_changes = []
        if st.session_state.fr_grid_original is not None and st.session_state.fr_grid_loaded is not None:
            current_changes = compute_fast_reschedule_changes(st.session_state.fr_grid_original, st.session_state.fr_grid_loaded)

        current_filter_signature = (
            fr_search,
            fr_subject,
            fr_work_type,
            fr_priority,
            fr_item_type,
            fr_project,
            fr_open_only,
            fr_limit,
            fr_sort_by,
            fr_sort_direction,
        )

        if "fr_last_filter_signature" not in st.session_state:
            st.session_state.fr_last_filter_signature = None

        discard_grid_clicked = st.button("Discard Unsaved Changes", key="fr_discard_grid")

        if discard_grid_clicked:
            if st.session_state.fr_grid_original is not None:
                st.session_state.fr_grid_loaded = st.session_state.fr_grid_original.copy(deep=True)
                current_changes = []
                st.rerun()

        filters_changed = st.session_state.fr_last_filter_signature != current_filter_signature

        if st.session_state.fr_grid_loaded is None:
            editor_df = build_fast_reschedule_editor_df(filtered_fr_df, project_name_map())
            editor_df = sort_fast_reschedule_editor_df(editor_df, fr_sort_by, fr_sort_direction)
            st.session_state.fr_grid_original = editor_df.copy(deep=True)
            st.session_state.fr_grid_loaded = editor_df.copy(deep=True)
            st.session_state.fr_grid_meta = filter_summary
            st.session_state.fr_last_filter_signature = current_filter_signature
            current_changes = []
        elif filters_changed and not current_changes:
            editor_df = build_fast_reschedule_editor_df(filtered_fr_df, project_name_map())
            editor_df = sort_fast_reschedule_editor_df(editor_df, fr_sort_by, fr_sort_direction)
            st.session_state.fr_grid_original = editor_df.copy(deep=True)
            st.session_state.fr_grid_loaded = editor_df.copy(deep=True)
            st.session_state.fr_grid_meta = filter_summary
            st.session_state.fr_last_filter_signature = current_filter_signature
            current_changes = []
        elif filters_changed and current_changes:
            st.warning("You have unsaved changes. Save or discard them before changing the filter results shown in the grid.")

        if st.session_state.fr_grid_loaded is None:
            st.info("Set your filters to start batch editing.")
        else:
            st.caption(st.session_state.fr_grid_meta or "Loaded grid")
            st.write("Edit the Title, Appointment, Reminder, Critical Due Date, Complete, and Delete fields below. The Priority column is shown for confirmation of your selected priority filter. Then click Save All Changes once. Do not switch filters until you save or discard your unsaved changes.")

            edited_df = st.data_editor(
                st.session_state.fr_grid_loaded,
                key="fast_reschedule_grid_editor",
                hide_index=True,
                use_container_width=True,
                num_rows="fixed",
                row_height=84,
                disabled=["ID", "Priority", "Subject", "Work Type", "Idea", "Notes Preview"],
                column_config={
                    "ID": st.column_config.NumberColumn("ID", width="small"),
                    "Title": st.column_config.TextColumn("Title", width="large"),
                    "Priority": st.column_config.TextColumn("Priority", width="small"),
                    "Subject": st.column_config.TextColumn("Subject", width="medium"),
                    "Work Type": st.column_config.TextColumn("Work Type", width="small"),
                    "Idea": st.column_config.TextColumn("Idea", width="medium"),
                    "Appointment": st.column_config.CheckboxColumn("Appointment", width="small"),
                    "Appointment Date": st.column_config.DateColumn("Appointment Date", format="MM/DD/YYYY"),
                    "Reminder": st.column_config.CheckboxColumn("Reminder", width="small"),
                    "Reminder Date": st.column_config.DateColumn("Reminder Date", format="MM/DD/YYYY"),
                    "Critical Due Date": st.column_config.DateColumn("Critical Due Date", format="MM/DD/YYYY"),
                    "Complete": st.column_config.CheckboxColumn("Complete", width="small", help="Check this and Save All Changes to mark the item complete."),
                    "Delete": st.column_config.CheckboxColumn("Delete", width="small", help="Check this and Save All Changes to delete the item."),
                    "Notes Preview": st.column_config.TextColumn("Notes Preview", width="large"),
                },
            )
            st.session_state.fr_grid_loaded = edited_df.copy(deep=True)
            current_changes = compute_fast_reschedule_changes(st.session_state.fr_grid_original, edited_df)

            fast_pdf_bytes = build_fast_reschedule_view_pdf(edited_df, "Fast Reschedule Current View")
            st.download_button(
                "Download / Print Current Fast Reschedule View PDF",
                data=fast_pdf_bytes,
                file_name="fast_reschedule_current_view.pdf",
                mime="application/pdf",
                key="fast_reschedule_current_view_pdf",
            )

            st.markdown("#### Copy / Bulk Update Item Titles")
            current_titles_text = "\n\n".join(
                f"###ID:{int(row['ID'])}###\n{str(row.get('Title', '') or '')}"
                for _, row in edited_df.iterrows()
            )
            st.text_area(
                "Copy Item # + Titles into Word",
                value=current_titles_text,
                height=260,
                key="fast_reschedule_copy_titles_text",
            )

            with st.expander("Paste edited Item # + Titles back into the system"):
                st.caption(
                    "Safer format: each item starts with ###ID:123### on its own line. "
                    "Everything after that marker becomes the title until the next ###ID:### marker. "
                    "This supports long text, paragraphs, and multiple lines."
                )
                pasted_title_updates = st.text_area(
                    "Paste edited titles here",
                    value="",
                    height=320,
                    key="fast_reschedule_bulk_title_updates",
                    placeholder="Example:\n###ID:123###\nUpdated title text can be long.\nIt can also have multiple lines.\n\n###ID:124###\nAnother updated title",
                )
                preview_title_updates = []
                skipped_title_update_lines = []
                duplicate_title_update_ids = []
                missing_from_paste_ids = []
                if pasted_title_updates.strip():
                    valid_ids = set(int(x) for x in edited_df["ID"].tolist())
                    original_title_map = {
                        int(row["ID"]): str(row.get("Title", "") or "")
                        for _, row in edited_df.iterrows()
                    }
                    seen_ids = set()
                    marker_pattern = re.compile(r"(?m)^\s*###\s*ID\s*:\s*(\d+)\s*###\s*$")
                    marker_matches = list(marker_pattern.finditer(pasted_title_updates))

                    if marker_matches:
                        for marker_index, marker in enumerate(marker_matches):
                            item_id = int(marker.group(1))
                            content_start = marker.end()
                            content_end = marker_matches[marker_index + 1].start() if marker_index + 1 < len(marker_matches) else len(pasted_title_updates)
                            new_title = pasted_title_updates[content_start:content_end].strip()

                            if item_id in seen_ids:
                                duplicate_title_update_ids.append(item_id)
                                continue
                            seen_ids.add(item_id)

                            if item_id not in valid_ids:
                                skipped_title_update_lines.append(f"Item #{item_id}: not in the current Fast Reschedule view")
                                continue
                            if not new_title:
                                skipped_title_update_lines.append(f"Item #{item_id}: title text is blank")
                                continue
                            preview_title_updates.append({
                                "id": item_id,
                                "old_title": original_title_map.get(item_id, ""),
                                "new_title": new_title,
                            })
                    else:
                        st.info("No ###ID:123### markers found. Trying older one-line format as a fallback.")
                        for line_number, raw_line in enumerate(pasted_title_updates.splitlines(), start=1):
                            line = raw_line.strip()
                            if not line:
                                continue
                            match = re.match(r"^#?\s*(\d+)\s*[-–—:]\s*(.+)$", line)
                            if not match:
                                skipped_title_update_lines.append(f"Line {line_number}: could not read item number and title")
                                continue
                            item_id = int(match.group(1))
                            new_title = match.group(2).strip()
                            if item_id in seen_ids:
                                duplicate_title_update_ids.append(item_id)
                                continue
                            seen_ids.add(item_id)
                            if item_id not in valid_ids:
                                skipped_title_update_lines.append(f"Line {line_number}: item #{item_id} is not in the current Fast Reschedule view")
                                continue
                            if not new_title:
                                skipped_title_update_lines.append(f"Line {line_number}: title is blank")
                                continue
                            preview_title_updates.append({
                                "id": item_id,
                                "old_title": original_title_map.get(item_id, ""),
                                "new_title": new_title,
                            })

                    missing_from_paste_ids = sorted(valid_ids - seen_ids)

                    if duplicate_title_update_ids:
                        st.error("Duplicate item ID(s) found and skipped: " + ", ".join(str(x) for x in sorted(set(duplicate_title_update_ids))))
                    if skipped_title_update_lines:
                        st.warning("Some pasted entries were skipped:\n" + "\n".join(skipped_title_update_lines))
                    if missing_from_paste_ids:
                        st.caption(
                            f"Note: {len(missing_from_paste_ids)} item(s) from the current view were not included in the paste. "
                            "That is okay if you only intended to update selected items."
                        )

                    if preview_title_updates:
                        preview_df = pd.DataFrame(preview_title_updates)
                        preview_df["changed"] = preview_df["old_title"] != preview_df["new_title"]
                        st.write("Preview changes before applying:")
                        st.dataframe(
                            preview_df[["id", "changed", "old_title", "new_title"]],
                            use_container_width=True,
                            hide_index=True,
                        )
                        preview_title_updates = [
                            row for row in preview_title_updates
                            if row["old_title"] != row["new_title"]
                        ]
                        if not preview_title_updates:
                            st.info("No actual title changes were found.")

                apply_bulk_titles = st.button(
                    "Apply Bulk Title Updates",
                    key="fast_reschedule_apply_bulk_title_updates",
                    disabled=not bool(preview_title_updates) or bool(duplicate_title_update_ids),
                )
                if apply_bulk_titles:
                    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                    backup_path = DATA_DIR / f"bulk_title_update_backup_{timestamp}.csv"
                    backup_rows = []
                    with conn() as c:
                        cur = c.cursor()
                        for update in preview_title_updates:
                            backup_rows.append({
                                "id": update["id"],
                                "old_title": update["old_title"],
                                "new_title": update["new_title"],
                                "backup_created": timestamp,
                            })
                            cur.execute("UPDATE items SET title=? WHERE id=?", (update["new_title"], update["id"]))
                        c.commit()
                    if backup_rows:
                        pd.DataFrame(backup_rows).to_csv(backup_path, index=False)
                    invalidate_schedule_caches()
                    st.session_state.fr_grid_original = None
                    st.session_state.fr_grid_loaded = None
                    st.session_state.fr_last_filter_signature = None
                    st.success(f"Updated {len(preview_title_updates)} item title(s). Backup saved to: {backup_path}")
                    st.rerun()

            if current_changes:
                st.warning(f"Unsaved changes: {len(current_changes)} item(s). Save or discard before leaving this page or loading a new filtered grid.")
            else:
                st.success("No unsaved changes.")

            saveb1, saveb2, saveb3 = st.columns([1.4, 1.3, 5])
            with saveb1:
                save_all_clicked = st.button("Save All Changes", key="fr_save_all_top")
            with saveb2:
                discard_bottom_clicked = st.button("Discard Unsaved", key="fr_discard_bottom")

            if discard_bottom_clicked:
                st.session_state.fr_grid_loaded = st.session_state.fr_grid_original.copy(deep=True)
                st.rerun()

            if save_all_clicked:
                saved_count = save_dates_batch(current_changes)
                st.session_state.fr_grid_original = None
                st.session_state.fr_grid_loaded = None
                st.session_state.fr_last_filter_signature = None
                current_changes = []
                st.success(f"Saved changes for {saved_count} item(s).")
                st.rerun()

            st.markdown("#### Changed Rows Preview")
            if current_changes:
                preview_df = pd.DataFrame(current_changes)
                preview_cols = [c for c in ["id", "title", "is_appointment", "appointment_date", "is_reminder", "reminder_date", "critical_due_date", "complete", "delete"] if c in preview_df.columns]
                st.dataframe(preview_df[preview_cols], use_container_width=True, hide_index=True)
            else:
                st.caption("No pending changes.")


if current_page == "Subject Manager":
    st.subheader("Subject Manager")
    st.write("Create, rename, and delete subject categories used throughout the app.")
    current_subjects = get_subjects()

    sm1, sm2 = st.columns(2)

    with sm1:
        st.markdown("### Add Subject")
        new_subject_name = st.text_input("New subject name", key="subject_add_name")
        if st.button("Add Subject"):
            ok, msg = add_subject(new_subject_name)
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

        st.markdown("---")
        st.markdown("### Rename Subject")
        rename_old = st.selectbox("Choose subject to rename", current_subjects, index=0, key="subject_rename_old")
        rename_new = st.text_input("New name", key="subject_rename_new")
        if st.button("Rename Subject"):
            ok, msg = rename_subject(rename_old, rename_new)
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

    with sm2:
        st.markdown("### Delete Subject")
        delete_subject_name = st.selectbox("Choose subject to delete", current_subjects, index=0, key="subject_delete_name")
        replacement_choices = [s for s in current_subjects if s != delete_subject_name]
        replacement_subject = st.selectbox("Move existing items to", replacement_choices, index=0 if replacement_choices else None, key="subject_delete_replacement")
        if st.button("Delete Subject"):
            ok, msg = delete_subject(delete_subject_name, replacement_subject if replacement_choices else "Unassigned")
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

    st.markdown("---")
    st.markdown("### Current Subjects")
    st.dataframe(pd.DataFrame({"Subject": get_subjects()}), use_container_width=True, hide_index=True)
    subjects_pdf = build_subjects_pdf()
    st.download_button(
        "Download / Print Subjects PDF",
        data=subjects_pdf,
        file_name="current_subjects_alphabetical.pdf",
        mime="application/pdf",
        key="download_subjects_pdf",
    )




if current_page == "Work Type Manager":
    st.subheader("Work Type Manager")
    st.write("Create, rename, and delete work types used throughout the app.")
    current_work_types = get_work_types()

    wm1, wm2 = st.columns(2)

    with wm1:
        st.markdown("### Add Work Type")
        new_work_type_name = st.text_input("New work type name", key="work_type_add_name")
        if st.button("Add Work Type"):
            ok, msg = add_work_type(new_work_type_name)
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

        st.markdown("---")
        st.markdown("### Rename Work Type")
        rename_old_wt = st.selectbox("Choose work type to rename", current_work_types, index=0, key="work_type_rename_old")
        rename_new_wt = st.text_input("New work type", key="work_type_rename_new")
        if st.button("Rename Work Type"):
            ok, msg = rename_work_type(rename_old_wt, rename_new_wt)
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

    with wm2:
        st.markdown("### Delete Work Type")
        delete_work_type_name = st.selectbox("Choose work type to delete", current_work_types, index=0, key="work_type_delete_name")
        replacement_choices = [w for w in current_work_types if w != delete_work_type_name]
        replacement_work_type = st.selectbox("Move existing items to", replacement_choices, index=0 if replacement_choices else None, key="work_type_delete_replacement")
        if st.button("Delete Work Type"):
            ok, msg = delete_work_type(delete_work_type_name, replacement_work_type if replacement_choices else "Unassigned")
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

    st.markdown("---")
    st.markdown("### Current Work Types")
    st.dataframe(pd.DataFrame({"Work Type": get_work_types()}), use_container_width=True, hide_index=True)



if current_page == "Priority Manager":
    st.subheader("Priority Manager")
    st.write("Create, rename, and delete priority categories used throughout the app.")
    current_priorities = get_priority_types()

    pm1, pm2 = st.columns(2)

    with pm1:
        st.markdown("### Add Priority")
        new_priority_name = st.text_input("New priority name", key="priority_add_name")
        if st.button("Add Priority"):
            ok, msg = add_priority_type(new_priority_name)
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

        st.markdown("---")
        st.markdown("### Rename Priority")
        rename_old_prio = st.selectbox("Choose priority to rename", current_priorities, index=0, key="priority_rename_old")
        rename_new_prio = st.text_input("New priority", key="priority_rename_new")
        if st.button("Rename Priority"):
            ok, msg = rename_priority_type(rename_old_prio, rename_new_prio)
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

    with pm2:
        st.markdown("### Delete Priority")
        delete_priority_name = st.selectbox("Choose priority to delete", current_priorities, index=0, key="priority_delete_name")
        replacement_choices = [p for p in current_priorities if p != delete_priority_name]
        default_replacement_index = replacement_choices.index("Task Repository") if "Task Repository" in replacement_choices else 0
        replacement_priority = st.selectbox("Move existing items to", replacement_choices, index=default_replacement_index if replacement_choices else None, key="priority_delete_replacement")
        if st.button("Delete Priority"):
            ok, msg = delete_priority_type(delete_priority_name, replacement_priority if replacement_choices else "Task Repository")
            if ok:
                st.success(msg)
            else:
                st.warning(msg)
            st.rerun()

    st.markdown("---")
    st.markdown("### Current Priorities")
    st.dataframe(pd.DataFrame({"Priority": get_priority_types()}), use_container_width=True, hide_index=True)



if current_page == "Assign Tasks":
    st.subheader("Assign Tasks")
    st.write("Create delegated tasks for a specific assignee, or pull wording from an existing item and turn it into a new assigned task.")

    assignee_choices = get_assignee_options(include_unassigned=False)
    if not assignee_choices:
        st.info("Create at least one assignee on the Assignee Manager page before assigning tasks.")
    else:
        if "assign_task_text_prefill" not in st.session_state:
            st.session_state.assign_task_text_prefill = ""
        if "assign_task_reset_counter" not in st.session_state:
            st.session_state.assign_task_reset_counter = 0

        st.markdown("### New Assigned Task")
        at1, at2 = st.columns([2, 3])
        with at1:
            assign_new_assignee = st.selectbox("Assignee", assignee_choices, index=0, key=f"assign_new_assignee_{st.session_state.assign_task_reset_counter}")
        with at2:
            st.caption("This creates a new task directly assigned to the selected person.")

        assigned_task_text = st.text_area("Task to assign", value=st.session_state.assign_task_text_prefill, height=115, key=f"assign_new_task_text_{st.session_state.assign_task_reset_counter}")

        af1, af2, af3 = st.columns(3)
        with af1:
            assign_new_work_type = st.selectbox("Work Type", get_work_types(), index=0, key=f"assign_new_work_type_{st.session_state.assign_task_reset_counter}")
        with af2:
            assign_new_subject = st.selectbox("Subject", get_subjects(), index=0, key=f"assign_new_subject_{st.session_state.assign_task_reset_counter}")
        with af3:
            assign_new_priority = st.selectbox("Priority", get_priority_types(), index=0, key=f"assign_new_priority_{st.session_state.assign_task_reset_counter}")

        assigned_task_notes = st.text_area("Notes / internal detail (optional)", height=70, key=f"assign_new_notes_{st.session_state.assign_task_reset_counter}")

        ad1, ad2, ad3, ad4, ad5 = st.columns(5)
        with ad1:
            assign_is_appointment = st.checkbox("Appointment", key=f"assign_is_appointment_{st.session_state.assign_task_reset_counter}")
        with ad2:
            assign_appointment_date = st.date_input("Appointment date", value=date.today(), key=f"assign_appointment_date_{st.session_state.assign_task_reset_counter}", disabled=not assign_is_appointment)
        with ad3:
            assign_is_reminder = st.checkbox("Reminder", key=f"assign_is_reminder_{st.session_state.assign_task_reset_counter}")
        with ad4:
            assign_reminder_date = st.date_input("Reminder date", value=date.today(), key=f"assign_reminder_date_{st.session_state.assign_task_reset_counter}", disabled=not assign_is_reminder)
        with ad5:
            assign_has_critical_due = st.checkbox("Critical Due", key=f"assign_has_critical_due_{st.session_state.assign_task_reset_counter}")
            assign_critical_due_date = st.date_input("Critical due", value=date.today(), key=f"assign_critical_due_date_{st.session_state.assign_task_reset_counter}", disabled=not assign_has_critical_due)

        if st.button("Create Assigned Task", key=f"create_assigned_task_{st.session_state.assign_task_reset_counter}"):
            ok, msg = add_delegated_task(assigned_task_text, assign_new_assignee, work_type=assign_new_work_type, subject=assign_new_subject, priority_type=assign_new_priority, notes=assigned_task_notes, is_appointment=int(bool(assign_is_appointment)), appointment_date=format_date_safe(assign_appointment_date) if assign_is_appointment else "", is_reminder=int(bool(assign_is_reminder)), reminder_date=format_date_safe(assign_reminder_date) if assign_is_reminder else "", critical_due_date=format_date_safe(assign_critical_due_date) if assign_has_critical_due else "", assignee_task_text=assigned_task_text)
            if ok:
                st.success(msg)
                st.session_state.assign_task_text_prefill = ""
                st.session_state.assign_task_reset_counter += 1
                st.rerun()
            else:
                st.error(msg)

        st.markdown("---")
        st.markdown("### Pull Wording From Existing Items")
        st.write("Search the master item list, choose an item, then copy its wording into the new assigned task box above. You can edit the wording before saving.")
        pt1, pt2, pt3 = st.columns([2, 2, 3])
        with pt1:
            pull_subject = st.selectbox("Filter by Subject", ["All"] + get_subjects(), index=0, key="assign_pull_subject")
        with pt2:
            pull_priority = st.selectbox("Filter by Priority", ["All"] + get_priority_types(), index=0, key="assign_pull_priority")
        with pt3:
            pull_search = st.text_input("Search existing item text", key="assign_pull_search")

        pull_df = load_all().copy()
        if not pull_df.empty:
            if pull_subject != "All":
                pull_df = pull_df[pull_df["subject"].fillna("Unassigned") == pull_subject].copy()
            if pull_priority != "All":
                pull_df = pull_df[pull_df["priority_type"].fillna("Task Repository") == pull_priority].copy()
            if pull_search.strip():
                q = pull_search.strip().lower()
                pull_df = pull_df[pull_df.apply(lambda row: q in str(row.get("title", "")).lower() or q in str(row.get("notes", "")).lower() or q in str(row.get("subject", "")).lower(), axis=1)].copy()
            pull_df = pull_df.sort_values(by=["id"], ascending=False).head(100).copy()

        if pull_df.empty:
            st.info("No existing items match those filters.")
        else:
            label_to_text = {}
            labels = []
            for _, row in pull_df.iterrows():
                label = f"#{int(row['id'])} | {str(row.get('subject', 'Unassigned') or 'Unassigned')[:40]} | {str(row.get('title', '') or '')[:120]}"
                label_to_text[label] = str(row.get("assignee_task_text", "") or "").strip() or str(row.get("title", "") or "").strip()
                labels.append(label)
            selected_source_label = st.selectbox("Choose existing item to copy from", labels, key="assign_pull_selected_item")
            if selected_source_label:
                source_text = label_to_text.get(selected_source_label, "")
                st.text_area("Selected existing item text", value=source_text, height=90, key="assign_pull_preview", disabled=True)
                if st.button("Use This Text In New Assigned Task", key="assign_copy_text_button"):
                    st.session_state.assign_task_text_prefill = source_text
                    st.session_state.assign_task_reset_counter += 1
                    st.rerun()

        st.markdown("---")
        st.markdown("### Print Tasks by Assignee")
        pr1, pr2, pr3, pr4 = st.columns(4)
        with pr1:
            print_assignee = st.selectbox("Assignee", assignee_choices, index=0, key="assign_print_assignee")
        with pr2:
            print_priority = st.selectbox("Priority", ["All"] + get_priority_types(), index=0, key="assign_print_priority")
        with pr3:
            print_subject = st.selectbox("Subject", ["All"] + get_subjects(), index=0, key="assign_print_subject")
        with pr4:
            print_include_completed = st.checkbox("Include completed", value=False, key="assign_print_include_completed")

        assignee_report_df = build_assignee_report_df(assignee_name=print_assignee, priority_filter=print_priority, subject_filter=print_subject, include_completed=print_include_completed)
        display_cols = ["Assignee", "Item ID", "Task Text", "Subject", "Priority", "Critical Due Date", "Appointment", "Reminder"]
        st.caption(f"Report date: {format_date_safe(date.today())} | Report rows: {len(assignee_report_df)}")
        if assignee_report_df.empty:
            st.info("No assigned tasks matched the selected report filters.")
        else:
            st.dataframe(assignee_report_df[display_cols], use_container_width=True, hide_index=True)
            report_date_text = format_date_safe(date.today())
            assignee_pdf = build_assignee_worklist_pdf(assignee_report_df, report_title=f"{print_assignee} - Assigned Tasks - {report_date_text}")
            safe_name = print_assignee.replace(" ", "_").replace("&", "and")
            safe_date = report_date_text.replace("/", "-")
            st.download_button("Download / Print Assignee Task Report PDF", data=assignee_pdf, file_name=f"assigned_tasks_{safe_name}_{safe_date}.pdf", mime="application/pdf", key="download_assign_tasks_pdf")


if current_page == "Assignee Manager":
    st.subheader("Assignee Manager")
    st.write("Add, rename, or delete assignees. Task assignment is handled on the Assign Tasks page.")

    st.markdown("### Manage Assignees")
    am1, am2, am3 = st.columns(3)

    with am1:
        new_assignee = st.text_input("New assignee name", key=f"assignee_add_name_{st.session_state.assignee_add_reset_counter}")
        if st.button("Add Assignee", key="add_assignee_button"):
            ok, msg = add_assignee(new_assignee)
            if ok:
                st.success(msg)
                reset_assignee_add_form()
                st.rerun()
            else:
                st.error(msg)

    assignee_options_manage = get_assignee_options(include_unassigned=False)
    with am2:
        if assignee_options_manage:
            old_assignee_name = st.selectbox("Assignee to rename", assignee_options_manage, key="assignee_rename_old")
            new_assignee_name = st.text_input("New assignee name", key="assignee_rename_new")
            if st.button("Rename Assignee", key="rename_assignee_button"):
                ok, msg = rename_assignee(old_assignee_name, new_assignee_name)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)
        else:
            st.info("No assignees created yet.")

    with am3:
        if assignee_options_manage:
            delete_assignee_name = st.selectbox("Assignee to delete", assignee_options_manage, key="assignee_delete_name")
            confirm_assignee_delete = st.checkbox("Confirm assignee delete", key="confirm_assignee_delete")
            if st.button("Delete Assignee", key="delete_assignee_button", disabled=not confirm_assignee_delete):
                ok, msg = delete_assignee(delete_assignee_name)
                if ok:
                    st.success(msg)
                    st.rerun()
                else:
                    st.error(msg)

    st.markdown("---")
    st.markdown("### Current Assignees")
    current_assignees_df = get_assignees_df(include_inactive=True)
    if current_assignees_df.empty:
        st.info("No assignees created yet.")
    else:
        st.dataframe(current_assignees_df[["id", "name", "active"]], use_container_width=True, hide_index=True)

if current_page == "Completed Items":
    st.subheader("Completed Items")
    df_completed = load_completed()
    if df_completed.empty:
        st.info("No completed items yet.")
    else:
        display_cols = [c for c in ["id","title","work_type","subject","priority_type","critical_due_date"] if c in df_completed.columns]
        st.dataframe(df_completed[display_cols], use_container_width=True, hide_index=True)
        for r in df_completed.itertuples():
            c1, c2 = st.columns([8,1])
            c1.write(f"- {r.title}")
            if c2.button("Reopen", key=f"completed_reopen_{r.id}"):
                mark_open(r.id)
                st.rerun()



if current_page == "Driving List":
    st.subheader("Driving List")
    st.write("View all open driving together so they can be grouped into one trip.")
    driving_df = load_all()
    if driving_df.empty:
        st.info("No items yet.")
    else:
        driving_df = driving_df[(driving_df["work_type"] == "Driving") & (driving_df["state"].fillna("Open") != "Complete")].copy()
        if driving_df.empty:
            st.info("No open driving found.")
        else:
            driving_subject = st.selectbox("Filter driving by Subject", ["All"] + get_subjects(), index=0, key="driving_subject_filter")

            if driving_subject != "All":
                driving_df = driving_df[driving_df["subject"] == driving_subject]

            driving_df = driving_df.sort_values(by=["subject", "title", "id"])
            render_editable_item_results(driving_df, "driving")