# signal_permits.py
# Checks NJ construction permit activity for every property
# in the properties table. Writes PERMIT_X signals for properties
# with no permit activity in over 24 months that also have other
# signals present. To change the inactivity threshold edit this file.
# Data source: data.nj.gov NJ Construction Permit Data (Socrata API)

import requests
from datetime import date, datetime
from db import get_connection

# NJ Construction Permit Data API on data.nj.gov
# Runs on Socrata — supports standard SoQL queries
PERMITS_API = "https://data.nj.gov/resource/w9se-dmra.json"

# How many months of permit inactivity before flagging
INACTIVITY_MONTHS = 24

def get_last_permit_date(muni_name, block, lot):
    """
    Queries the NJ permits API for the most recent permit
    activity on a specific property identified by municipality,
    block, and lot. Returns the date of the last permit or None.
    """
    params = {
        "$where": f"municipality='{muni_name}' AND block='{block}' AND lot='{lot}'",
        "$order": "issue_date DESC",
        "$limit": 1
    }
    try:
        response = requests.get(PERMITS_API, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        if data and "issue_date" in data[0]:
            date_str = data[0]["issue_date"][:10]
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        return None
    except Exception as e:
        return None

def months_since(past_date):
    """
    Returns the number of months between a past date and today.
    """
    today = date.today()
    return (today.year - past_date.year) * 12 + (today.month - past_date.month)

def signal_exists(conn, property_id, signal_code):
    """
    Returns True if an active signal of this type already exists
    for this property. Prevents duplicate signal entries.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT signal_id FROM signals
        WHERE property_id = %s
        AND signal_code = %s
        AND status = 'ACTIVE'
    """, (property_id, signal_code))
    result = cur.fetchone()
    cur.close()
    return result is not None

def write_signal(conn, property_id, signal_code, source, snapshot):
    """
    Writes a new signal row to the signals table.
    Only called after confirming signal does not already exist.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO signals (
            property_id, signal_code, status,
            detected_date, source_name, raw_snapshot
        )
        VALUES (%s, %s, 'ACTIVE', CURRENT_DATE, %s, %s)
    """, (property_id, signal_code, source, snapshot))
    conn.commit()
    cur.close()

def resolve_signal(conn, property_id, signal_code):
    """
    Marks an existing active signal as resolved.
    Called when a property that was flagged now shows recent activity.
    """
    cur = conn.cursor()
    cur.execute("""
        UPDATE signals
        SET status = 'RESOLVED', resolved_date = CURRENT_DATE
        WHERE property_id = %s
        AND signal_code = %s
        AND status = 'ACTIVE'
    """, (property_id, signal_code))
    conn.commit()
    cur.close()

def get_properties_to_check(conn):
    """
    Returns all properties from the properties table.
    Ordered by muni_name to batch API calls efficiently.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT property_id, muni_name, block, lot
        FROM properties
        ORDER BY muni_name, block, lot
    """)
    rows = cur.fetchall()
    cur.close()
    return rows

def run_permit_signals():
    """
    Main entry point. Checks every property for permit inactivity
    and writes or resolves PERMIT_X signals accordingly.
    """
    print("=== PERMIT SIGNAL CHECK ===")
    conn = get_connection()
    properties = get_properties_to_check(conn)
    print(f"Properties to check: {len(properties)}")

    flagged = 0
    resolved = 0
    checked = 0

    for prop in properties:
        property_id = prop["property_id"]
        muni_name = prop["muni_name"]
        block = prop["block"]
        lot = prop["lot"]

        last_permit = get_last_permit_date(muni_name, block, lot)
        has_active_signal = signal_exists(conn, property_id, "PERMIT_X")

        if last_permit is None:
            # No permit history found at all
            # Only flag if property has been in database long enough
            # to reasonably expect permit activity
            if not has_active_signal:
                snapshot = f"No permit history found in NJ DCA database"
                write_signal(conn, property_id, "PERMIT_X", "NJ DCA Permits API", snapshot)
                flagged += 1

        elif months_since(last_permit) >= INACTIVITY_MONTHS:
            # Last permit was over threshold months ago
            if not has_active_signal:
                snapshot = f"Last permit: {last_permit} ({months_since(last_permit)} months ago)"
                write_signal(conn, property_id, "PERMIT_X", "NJ DCA Permits API", snapshot)
                flagged += 1

        else:
            # Recent permit activity — resolve signal if one exists
            if has_active_signal:
                resolve_signal(conn, property_id, "PERMIT_X")
                resolved += 1

        checked += 1
        if checked % 100 == 0:
            print(f"  Checked: {checked} | Flagged: {flagged} | Resolved: {resolved}")

    conn.close()
    print(f"\nCompleted permit signal check.")
    print(f"Total checked: {checked}")
    print(f"New PERMIT_X signals: {flagged}")
    print(f"Resolved PERMIT_X signals: {resolved}")
    print("=== PERMIT CHECK COMPLETE ===")

if __name__ == "__main__":
    run_permit_signals()
