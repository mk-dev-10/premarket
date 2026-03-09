from db import get_connection
from ingest_modiv import run_ingestion
from signal_permits import run_permit_signals
from signal_llc import run_llc_signals
from scorer import run_scorer
from datetime import datetime

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def verify_database():
    log("Verifying database...")
    conn = get_connection()
    cur = conn.cursor()
    tables = ['properties', 'signals', 'scores', 'watchlists']
    for table in tables:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        count = cur.fetchone()
        log(f"  Table '{table}' — {count['count']} rows")
    cur.close()
    conn.close()
    log("Database verified.")

def run_pipeline():
    log("=" * 50)
    log("PREMARKET PIPELINE STARTING")
    log("=" * 50)

    try:
        verify_database()
    except Exception as e:
        log(f"DATABASE ERROR: {e}")
        log("Pipeline aborted.")
        return

    log("Starting MOD-IV ingestion...")
    try:
        run_ingestion()
        log("Ingestion complete.")
    except Exception as e:
        log(f"INGESTION ERROR: {e}")

    log("Starting signal detection...")

    try:
        run_permit_signals()
        log("Permit signals complete.")
    except Exception as e:
        log(f"PERMIT SIGNAL ERROR: {e}")

    try:
        run_llc_signals()
        log("LLC signals complete.")
    except Exception as e:
        log(f"LLC SIGNAL ERROR: {e}")

    log("Starting scorer...")
    try:
        run_scorer()
        log("Scoring complete.")
    except Exception as e:
        log(f"SCORER ERROR: {e}")

    log("=" * 50)
    log("PREMARKET PIPELINE COMPLETE")
    log("=" * 50)

if __name__ == "__main__":
    run_pipeline()
