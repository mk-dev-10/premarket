# signal_base.py
# Shared framework for all signal checkers.
# Every signal file imports from here instead of duplicating logic.
# To add a new signal checker: create a new file, import these utilities,
# define your source query and detection logic, nothing else needed.

import psycopg2.extras
from db import get_connection
from datetime import date

def signal_exists(conn, property_id, signal_code):
    """
    Returns True if an active signal already exists
    for this property and signal type.
    Prevents duplicate signals being written.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT signal_id FROM signals
        WHERE property_id = %s
        AND signal_code = %s
        AND status = 'ACTIVE'
    """, (property_id, signal_code))
    result = cur.fetchone()
    cur.close()
    return result is not None

def write_signal(conn, property_id, signal_code, source_name, raw_snapshot):
    """
    Writes a new active signal to the signals table.
    Only call this after confirming signal does not already exist.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO signals (
            property_id, signal_code, status,
            detected_date, source_name, raw_snapshot
        )
        VALUES (%s, %s, 'ACTIVE', CURRENT_DATE, %s, %s)
    """, (property_id, signal_code, source_name, raw_snapshot))
    conn.commit()
    cur.close()

def resolve_signal(conn, property_id, signal_code):
    """
    Marks an existing active signal as resolved.
    Call this when a signal condition no longer applies —
    for example an LLC that became active again.
    """
    cur = conn.cursor()
    cur.execute("""
        UPDATE signals
        SET status = 'RESOLVED',
            resolved_date = CURRENT_DATE
        WHERE property_id = %s
        AND signal_code = %s
        AND status = 'ACTIVE'
    """, (property_id, signal_code))
    conn.commit()
    cur.close()

def get_or_create_property(conn, block, lot, muni_code, muni_name,
                            county, address, class_code,
                            assessed_land, assessed_building,
                            last_tax_amount, lot_acres, year_built):
    """
    Returns the property_id for an existing property or creates
    a new one if it doesn't exist yet.
    This is how scan on demand works — properties only enter
    the database when a signal is detected, not before.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Check if property already exists
    cur.execute("""
        SELECT property_id FROM properties
        WHERE block = %s AND lot = %s AND muni_code = %s
    """, (block, lot, muni_code))
    result = cur.fetchone()

    if result:
        # Property exists — update last_updated timestamp
        cur.execute("""
            UPDATE properties
            SET last_updated = CURRENT_DATE
            WHERE property_id = %s
        """, (result["property_id"],))
        conn.commit()
        cur.close()
        return result["property_id"]

    # Property doesn't exist yet — create it
    cur.execute("""
        INSERT INTO properties (
            block, lot, muni_code, muni_name, county,
            address, class_code, assessed_land, assessed_building,
            last_tax_amount, lot_acres, year_built,
            first_detected, last_updated
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s,
            %s, %s, %s,
            CURRENT_DATE, CURRENT_DATE
        )
        RETURNING property_id
    """, (
        block, lot, muni_code, muni_name, county,
        address, class_code, assessed_land, assessed_building,
        last_tax_amount, lot_acres, year_built
    ))
    new_id = cur.fetchone()["property_id"]
    conn.commit()
    cur.close()
    return new_id

def get_active_signals(conn, property_id):
    """
    Returns all active signals for a given property.
    Used by the scorer and the frontend API.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT signal_code, detected_date, source_name, raw_snapshot
        FROM signals
        WHERE property_id = %s
        AND status = 'ACTIVE'
        ORDER BY detected_date DESC
    """, (property_id,))
    results = cur.fetchall()
    cur.close()
    return results

def log_run(signal_code, checked, flagged, resolved, errors):
    """
    Prints a clean summary at the end of every signal checker run.
    Consistent format across all signal files.
    """
    print(f"\n=== {signal_code} SIGNAL RUN COMPLETE ===")
    print(f"  Checked:  {checked}")
    print(f"  Flagged:  {flagged}")
    print(f"  Resolved: {resolved}")
    print(f"  Errors:   {errors}")
    print(f"  Date:     {date.today()}")
    print(f"=====================================\n")
