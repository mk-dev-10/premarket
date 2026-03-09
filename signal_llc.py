# signal_llc.py
# Detects LLC_I signals — properties owned by inactive or dissolved
# NJ business entities, suggesting ownership in limbo.
# Queries the NJ business registry by entity name.
# Runs daily via GitHub Actions since the registry updates in real time.
# Owner names sourced from Monmouth County OPRS per-property lookup
# rather than bulk data — fully compliant with Daniel's Law.

import requests
from db import get_connection
from config import LLC_API, MONMOUTH_OPRS
from signal_base import (
    signal_exists,
    write_signal,
    resolve_signal,
    log_run
)
from datetime import date
import psycopg2.extras
import re

# NJ business registry status values that indicate inactive entity
INACTIVE_STATUSES = [
    "Dissolved", "Revoked", "Cancelled",
    "Inactive", "Suspended", "Forfeited"
]

# Keywords that suggest a business entity rather than an individual
ENTITY_KEYWORDS = [
    "LLC", "L.L.C", "INC", "CORP", "LP", "L.P",
    "LTD", "TRUST", "HOLDINGS", "PROPERTIES",
    "REALTY", "VENTURES", "ASSOCIATES", "GROUP",
    "PARTNERS", "PARTNERSHIP", "ENTERPRISES"
]

def get_owner_name(block, lot, muni_name):
    """
    Looks up owner name for a specific property from
    Monmouth County OPRS portal.
    Only called for properties that have cleared signal threshold —
    targeted lookup not bulk scraping.
    Returns owner name string or None if not found.
    """
    params = {
        "idx": "lot",
        "block": block,
        "lot": lot,
        "town": muni_name.upper(),
        "year": str(date.today().year)
    }

    try:
        response = requests.get(MONMOUTH_OPRS, params=params, timeout=15)
        response.raise_for_status()

        # Parse owner name from response
        # OPRS returns HTML — look for owner name field
        content = response.text
        if "Owner Name" in content or "OWNER" in content:
            # Extract owner name between relevant tags
            match = re.search(
                r'(?:Owner Name|OWNER)[^\w]*([A-Z][A-Z\s\.,&]+)',
                content,
                re.IGNORECASE
            )
            if match:
                return match.group(1).strip()

        return None

    except Exception:
        return None

def is_entity_owned(owner_name):
    """
    Returns True if owner name looks like a business entity.
    """
    if not owner_name:
        return False
    owner_upper = owner_name.upper()
    for keyword in ENTITY_KEYWORDS:
        if keyword.upper() in owner_upper:
            return True
    return False

def lookup_business_status(owner_name):
    """
    Queries NJ business registry for entity status.
    Returns status string or None if not found.
    """
    # Clean name for searching
    search_name = owner_name.upper()
    for keyword in ENTITY_KEYWORDS:
        search_name = search_name.replace(keyword.upper(), "").strip()
    search_name = search_name[:50]

    params = {
        "$where": f"upper(businessname) like '%{search_name}%'",
        "$limit": 5,
        "$order": "businessname ASC"
    }

    try:
        response = requests.get(LLC_API, params=params, timeout=15)
        response.raise_for_status()
        results = response.json()

        if not results:
            return None

        return {
            "name":   results[0].get("businessname", ""),
            "status": results[0].get("status", ""),
            "type":   results[0].get("businesstype", ""),
            "id":     results[0].get("businessid", "")
        }

    except Exception:
        return None

def get_candidate_properties(conn, limit=None):
    """
    Returns properties with at least one active signal
    that don't already have an LLC_I signal.
    We only look up ownership for properties already
    flagged by another signal checker.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    query = """
        SELECT DISTINCT p.property_id, p.block, p.lot,
               p.muni_name, p.muni_code, p.address
        FROM properties p
        INNER JOIN signals s ON s.property_id = p.property_id
        WHERE s.status = 'ACTIVE'
        ORDER BY p.property_id
    """

    if limit:
        query += f" LIMIT {limit}"

    cur.execute(query)
    results = cur.fetchall()
    cur.close()
    return results

def run_llc_signals(limit=None):
    """
    Main entry point. For every flagged property:
    1. Look up owner name from county portal
    2. Check if owner is a business entity
    3. Check entity status in NJ registry
    4. Write or resolve LLC_I signal accordingly
    """
    print("=== LLC SIGNAL CHECK ===")
    conn = get_connection()

    properties = get_candidate_properties(conn, limit=limit)
    print(f"Flagged properties to check: {len(properties)}")

    checked = 0
    flagged = 0
    resolved = 0
    not_entity = 0
    not_found = 0
    errors = 0

    for prop in properties:
        try:
            # Step 1 — get owner name from county portal
            owner_name = get_owner_name(
                prop["block"],
                prop["lot"],
                prop["muni_name"]
            )

            has_signal = signal_exists(conn, prop["property_id"], "LLC_I")

            # Step 2 — check if entity owned
            if not is_entity_owned(owner_name):
                not_entity += 1
                if has_signal:
                    resolve_signal(conn, prop["property_id"], "LLC_I")
                    resolved += 1
                checked += 1
                continue

            # Step 3 — look up entity status
            result = lookup_business_status(owner_name)

            if result is None:
                not_found += 1
                checked += 1
                continue

            # Step 4 — write or resolve signal
            if result["status"] in INACTIVE_STATUSES:
                if not has_signal:
                    snapshot = (
                        f"Owner: {owner_name} | "
                        f"Registry name: {result['name']} | "
                        f"Status: {result['status']} | "
                        f"Type: {result['type']} | "
                        f"ID: {result['id']}"
                    )
                    write_signal(
                        conn,
                        prop["property_id"],
                        "LLC_I",
                        "NJ Division of Revenue Business Registry",
                        snapshot
                    )
                    flagged += 1
            else:
                if has_signal:
                    resolve_signal(conn, prop["property_id"], "LLC_I")
                    resolved += 1

        except Exception as e:
            errors += 1

        checked += 1
        if checked % 25 == 0:
            print(f"  Checked: {checked} | Flagged: {flagged} | "
                  f"Resolved: {resolved} | Not entity: {not_entity}")

    conn.close()
    log_run("LLC_I", checked, flagged, resolved, errors)

if __name__ == "__main__":
    run_llc_signals()
