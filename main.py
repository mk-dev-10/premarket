# main.py
# The single entry point for the entire Premarket pipeline.
# Railway runs this file. It calls every other module in order.
# To add a new signal checker: import it and call it in run_pipeline().
# Nothing else needs to change when adding new features.

from db import get_connection
from ingest_modiv import run_ingestion
from signal_permits import run_permit_signals
from signal_llc import run_llc_signals
from scorer import run_scorer
from datetime import datetime

def log(message):
    """
    Prints a timestamped log message.
    Every major pipeline step gets logged with a timestamp.
    """
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

def verify_database():
    """
    Confirms database connection and all tables exist
    before running the pipeline. Fails fast if anything
    is wrong rather than running halfway.
    """
    log("Verifying database connection...")
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
    """
    Runs the full Premarket pipeline in order.
    Each step is independent — if one fails the error
    is caught and logged without stopping the rest.

    Order:
    1. Verify database
    2. Ingest new property data
    3. Run signal checkers
    4. Run scorer
    """
    log("=" * 50)
    log("PREMARKET PIPELINE STARTING")
    log("=" * 50)

    # Step 1 — Verify database
    try:
        verify_database()
    except Exception as e:
        log(f"DATABASE ERROR: {e}")
        log("Pipeline aborted — cannot continue without database.")
        return

    # Step 2 — Ingest property data
    log("Starting MOD-IV ingestion...")
    try:
        run_ingestion()
        log("MOD-IV ingestion complete.")
    except Exception as e:
        log(f"INGESTION ERROR: {e}")
        log("Continuing pipeline despite ingestion error.")

    # Step 3 — Run signal checkers
    log("Starting signal detection...")

    try:
        run_permit_signals()
        log("Permit signal check complete.")
    except Exception as e:
        log(f"PERMIT SIGNAL ERROR: {e}")

    try:
        run_llc_signals()
        log("LLC signal check complete.")
    except Exception as e:
        log(f"LLC SIGNAL ERROR: {e}")

    # Step 4 — Run scorer
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
```
