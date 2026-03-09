# signal_permits.py
# Detects PERMIT_X signals — properties with no permit activity
# for an unusually long time, suggesting neglect or abandonment.
# Queries the NJ permit database by municipality and cross references
# against our flagged properties by address matching.
# Runs weekly via GitHub Actions.

import requests
from db import get_connection
from config import PERMITS_API, PERMITS_INACTIVITY_YEARS, PROPERTY_CLASSES
from signal_base import (
    get_or_create_property,
    signal_exists,
    write_signal,
    resolve_signal,
    log_run
)
from datetime import date, datetime
import psycopg2.extras

def get_latest_permit_date(address, muni_name):
    """
    Queries the NJ permits API for the most recent permit
    on a given address and municipality.
    Returns the date of the most recent permit or None if not found.
    """
    # Clean address for searching
    search_address = address.upper().strip()
    search_muni = muni_name.upper().strip()

    params = {
        "$where": (
            f"upper(site_street) like '%{search_address[:30]}%' "
            f"AND upper(municipality_name) like '%{search_muni[:20]}%'"
        ),
        "$order": "issue_date DESC",
        "$limit": 1
    }

    try:
        response = requests.get(PERMITS_API, params=params, timeout=15)
        response.raise_for_status()
        results = response.json()

        if not results:
            return None

        issue_date = results[0].get("issue_date")
        if not issue_date:
            return None

        # Parse date — API returns ISO format
        return datetime.strptime(issue_date[:10], "%Y-%m-%d").date()

    except Exception as e:
        return None

def is_permit_inactive(latest_permit_date):
    """
    Returns True if the most recent permit is older than
    PERMITS_INACTIVITY_YEARS or if no permit exists at all.
    """
    if latest_permit_date is None:
        return True
    years_ago = (date.today() - latest_permit_date).days / 365
    return years_ago >= PERMITS_INACTIVITY_YEARS

def get_candidate_properties(conn, limit=None):
    """
    Returns properties from our database to check for permit inactivity.
    In scan on demand architecture these are properties that already
    have at least one other signal — we use permit inactivity as a
    corroborating signal not an entry point.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    query = """
        SELECT DISTINCT p.property_id, p.address, p.muni_name,
               p.block, p.lot, p.muni_code
        FROM properties p
        INNER JOIN signals s ON s.property_id = p.property_id
        WHERE s.status = 'ACTIVE'
        AND s.signal_code != 'PERMIT_X'
        ORDER BY p.property_id
    """

    if limit:
        query += f" LIMIT {limit}"

    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    return results

def run_permit_signals(limit=None):
    """
    Main entry point. Checks permit activity for properties
    that already have at least one other active signal.
    Adds PERMIT_X as a corroborating signal where applicable.
    """
    print("=== PERMIT SIGNAL CHECK ===")
    conn = get_connection()

    properties = get_candidate_properties(conn, limit=limit)
    print(f"Properties to check: {len(properties)}")

    checked = 0
    flagged = 0
    resolved = 0
    errors = 0

    for prop in properties:
        try:
            latest_permit = get_latest_permit_date(
                prop["address"],
                prop["muni_name"]
            )

            inactive = is_permit_inactive(latest_permit)
            has_signal = signal_exists(conn, prop["property_id"], "PERMIT_X")

            if inactive and not has_signal:
                snapshot = (
                    f"Last permit: {latest_permit or 'none found'} | "
                    f"Address: {prop['address']} | "
                    f"Municipality: {prop['muni_name']}"
                )
                write_signal(
                    conn,
                    prop["property_id"],
                    "PERMIT_X",
                    "NJ Building Permits API",
                    snapshot
                )
                flagged += 1

            elif not inactive and has_signal:
                resolve_signal(conn, prop["property_id"], "PERMIT_X")
                resolved += 1

        except Exception as e:
            errors += 1

        checked += 1
        if checked % 50 == 0:
            print(f"  Checked: {checked} | Flagged: {flagged} | Resolved: {resolved}")

    conn.close()
    log_run("PERMIT_X", checked, flagged, resolved, errors)

if __name__ == "__main__":
    run_permit_signals()
