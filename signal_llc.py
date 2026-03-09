# signal_llc.py
# Checks NJ business registry for LLC status on every property
# where the owner name suggests an LLC owns it.
# Writes LLC_I signals for inactive or dissolved entities.
# Data source: NJ Division of Revenue Business Registry

import requests
import re
from db import get_connection

# NJ Business Registry search endpoint
NJ_BUSINESS_API = "https://data.nj.gov/resource/p4ys-idjb.json"

# Keywords that indicate a property is owned by a business entity
LLC_KEYWORDS = [
    "LLC", "L.L.C", "INC", "CORP", "LP", "L.P",
    "LTD", "TRUST", "HOLDINGS", "PROPERTIES",
    "REALTY", "VENTURES", "ASSOCIATES", "GROUP",
    "PARTNERS", "PARTNERSHIP", "ENTERPRISES"
]

# Status values from NJ registry that indicate inactive entity
INACTIVE_STATUSES = [
    "Dissolved", "Revoked", "Cancelled",
    "Inactive", "Suspended", "Forfeited"
]

def is_likely_entity(owner_name):
    """
    Returns True if the owner name looks like a business entity
    rather than an individual person.
    """
    if not owner_name:
        return False
    owner_upper = owner_name.upper()
    for keyword in LLC_KEYWORDS:
        if keyword.upper() in owner_upper:
            return True
    return False

def lookup_business(business_name):
    """
    Queries the NJ business registry for a business name.
    Returns the status and entity type if found, None if not found.
    """
    # Clean the name for searching — remove common suffixes
    # to improve match rate
    search_name = business_name.upper()
    search_name = re.sub(r'\bLLC\b|\bL\.L\.C\b|\bINC\b|\bCORP\b', '', search_name).strip()
    search_name = search_name[:50]  # API has length limits

    params = {
        "$where": f"upper(businessname) like '%{search_name}%'",
        "$limit": 5,
        "$order": "businessname ASC"
    }

    try:
        response = requests.get(NJ_BUSINESS_API, params=params, timeout=15)
        response.raise_for_status()
        results = response.json()

        if not results:
            return None

        # Return the first matching result
        return {
            "name":   results[0].get("businessname", ""),
            "status": results[0].get("status", ""),
            "type":   results[0].get("businesstype", ""),
            "id":     results[0].get("businessid", "")
        }

    except Exception as e:
        return None

def signal_exists(conn, property_id, signal_code):
    """
    Returns True if an active signal already exists for this property.
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
    Writes a new active signal to the signals table.
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

def get_entity_owned_properties(conn):
    """
    Returns all properties where owner name suggests
    a business entity rather than an individual.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT property_id, owner_name
        FROM properties
        WHERE owner_name IS NOT NULL
        AND owner_name != ''
        ORDER BY owner_name
    """)
    rows = cur.fetchall()
    cur.close()
    return [r for r in rows if is_likely_entity(r["owner_name"])]

def run_llc_signals():
    """
    Main entry point. Checks all entity owned properties against
    the NJ business registry and writes LLC_I signals for
    inactive or dissolved entities.
    """
    print("=== LLC SIGNAL CHECK ===")
    conn = get_connection()

    properties = get_entity_owned_properties(conn)
    print(f"Entity owned properties to check: {len(properties)}")

    flagged = 0
    resolved = 0
    not_found = 0
    checked = 0

    for prop in properties:
        property_id = prop["property_id"]
        owner_name = prop["owner_name"]

        result = lookup_business(owner_name)
        has_active_signal = signal_exists(conn, property_id, "LLC_I")

        if result is None:
            not_found += 1

        elif result["status"] in INACTIVE_STATUSES:
            if not has_active_signal:
                snapshot = (
                    f"Business: {result['name']} | "
                    f"Status: {result['status']} | "
                    f"Type: {result['type']} | "
                    f"ID: {result['id']}"
                )
                write_signal(conn, property_id, "LLC_I",
                           "NJ Division of Revenue Business Registry",
                           snapshot)
                flagged += 1

        else:
            # Business is active — resolve signal if one exists
            if has_active_signal:
                resolve_signal(conn, property_id, "LLC_I")
                resolved += 1

        checked += 1
        if checked % 100 == 0:
            print(f"  Checked: {checked} | Flagged: {flagged} | "
                  f"Resolved: {resolved} | Not found: {not_found}")

    conn.close()
    print(f"\nCompleted LLC signal check.")
    print(f"Total checked: {checked}")
    print(f"New LLC_I signals: {flagged}")
    print(f"Resolved LLC_I signals: {resolved}")
    print(f"Entities not found in registry: {not_found}")
    print("=== LLC CHECK COMPLETE ===")

if __name__ == "__main__":
    run_llc_signals()
