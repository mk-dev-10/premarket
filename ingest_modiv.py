# ingest_modiv.py
# Pulls NJ MOD-IV property data and populates the properties table.
# This runs once at setup and then annually when MOD-IV updates.
# To add a new county: add it to COUNTIES in config.py only.

import requests
import json
from db import get_connection
from config import COUNTIES, PROPERTY_CLASSES, MIN_ASSESSED_VALUE

# NJ MOD-IV data is available through the NJ Open Data API
# This endpoint returns property records filtered by municipality
MODIV_API = "https://data.nj.gov/resource/9s6e-czrg.json"

def fetch_properties_for_county(county_name, limit=1000, offset=0):
    """
    Fetches a batch of properties from the NJ MOD-IV dataset
    for a given county name.
    Returns a list of property records.
    """
    params = {
        "$where": f"county_name='{county_name}'",
        "$limit": limit,
        "$offset": offset,
        "$order": "property_id ASC"
    }

    try:
        response = requests.get(MODIV_API, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {county_name}: {e}")
        return []

def insert_property(cur, prop):
    """
    Inserts a single property record into the properties table.
    If the property already exists (same block/lot/muni) it updates it.
    This means you can safely run this multiple times without duplicates.
    """
    sql = """
        INSERT INTO properties (
            block, lot, muni_code, muni_name, county,
            address, class_code, assessed_land,
            assessed_building, last_tax_amount,
            lot_acres, year_built, owner_name,
            owner_address, modiv_refreshed
        )
        VALUES (
            %(block)s, %(lot)s, %(muni_code)s, %(muni_name)s, %(county)s,
            %(address)s, %(class_code)s, %(assessed_land)s,
            %(assessed_building)s, %(last_tax_amount)s,
            %(lot_acres)s, %(year_built)s, %(owner_name)s,
            %(owner_address)s, CURRENT_DATE
        )
        ON CONFLICT (block, lot, muni_code)
        DO UPDATE SET
            assessed_land = EXCLUDED.assessed_land,
            assessed_building = EXCLUDED.assessed_building,
            last_tax_amount = EXCLUDED.last_tax_amount,
            owner_name = EXCLUDED.owner_name,
            owner_address = EXCLUDED.owner_address,
            modiv_refreshed = CURRENT_DATE
    """

    # Map API field names to our database field names
    # API fields may vary slightly — this handles the mapping cleanly
    record = {
        "block":              prop.get("block", "").strip(),
        "lot":                prop.get("lot", "").strip(),
        "muni_code":          prop.get("municipality_code", "").strip(),
        "muni_name":          prop.get("municipality_name", "").strip(),
        "county":             prop.get("county_name", "").strip(),
        "address":            prop.get("property_location", "").strip(),
        "class_code":         prop.get("property_class", "").strip(),
        "assessed_land":      float(prop.get("land_value", 0) or 0),
        "assessed_building":  float(prop.get("improvement_value", 0) or 0),
        "last_tax_amount":    float(prop.get("net_taxes", 0) or 0),
        "lot_acres":          float(prop.get("lot_size", 0) or 0),
        "year_built":         int(prop.get("year_constructed", 0) or 0) or None,
        "owner_name":         prop.get("owner_name", "").strip(),
        "owner_address":      prop.get("owner_address", "").strip(),
    }

    cur.execute(sql, record)

def filter_property(prop):
    """
    Returns True if a property should be included based on config settings.
    Returns False if it should be skipped.
    Edit filtering logic in config.py not here.
    """
    # Check property class
    class_code = prop.get("property_class", "").strip()
    if class_code not in PROPERTY_CLASSES:
        return False

    # Check minimum assessed value
    land_value = float(prop.get("land_value", 0) or 0)
    improvement_value = float(prop.get("improvement_value", 0) or 0)
    total_value = land_value + improvement_value
    if total_value < MIN_ASSESSED_VALUE:
        return False

    return True

def ingest_county(county_name):
    """
    Pulls all qualifying properties for a county and loads them
    into the properties table. Processes in batches of 1000 to
    avoid timeout issues with large counties.
    """
    print(f"\nStarting ingestion for {county_name} County...")

    conn = get_connection()
    cur = conn.cursor()

    total_inserted = 0
    total_skipped = 0
    offset = 0
    batch_size = 1000

    while True:
        print(f"  Fetching records {offset} to {offset + batch_size}...")
        records = fetch_properties_for_county(county_name, batch_size, offset)

        # If no records returned we have reached the end
        if not records:
            print(f"  No more records found at offset {offset}")
            break

        for prop in records:
            if filter_property(prop):
                try:
                    insert_property(cur, prop)
                    total_inserted += 1
                except Exception as e:
                    print(f"  Error inserting property: {e}")
                    total_skipped += 1
            else:
                total_skipped += 1

        conn.commit()
        print(f"  Batch committed. Inserted: {total_inserted} | Skipped: {total_skipped}")

        # If we got fewer records than the batch size we are done
        if len(records) < batch_size:
            break

        offset += batch_size

    cur.close()
    conn.close()
    print(f"\nCompleted {county_name} County.")
    print(f"Total inserted or updated: {total_inserted}")
    print(f"Total skipped: {total_skipped}")

def run_ingestion():
    """
    Runs ingestion for all counties listed in config.py.
    Add counties to config.py to expand coverage.
    """
    print("=== PREMARKET MOD-IV INGESTION ===")
    print(f"Counties to process: {COUNTIES}")
    print(f"Property classes: {PROPERTY_CLASSES}")

    for county in COUNTIES:
        ingest_county(county)

    print("\n=== INGESTION COMPLETE ===")

if __name__ == "__main__":
    run_ingestion()
