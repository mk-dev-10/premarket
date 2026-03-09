# ingest_modiv.py
# Pulls NJ parcel and MOD-IV data via the official NJ ArcGIS REST API.
# To add counties edit COUNTIES in config.py only.
# Field names verified against live API response March 2026.

import requests
from db import get_connection
from config import COUNTIES, PROPERTY_CLASSES, MIN_ASSESSED_VALUE

ARCGIS_API = "https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/arcgis/rest/services/Parcels_Composite_NJ_WM/FeatureServer/0/query"

OUTFIELDS = ",".join([
    "PAMS_PIN", "PCL_MUN", "PCLBLOCK", "PCLLOT",
    "COUNTY", "MUN_NAME", "PROP_LOC", "PROP_CLASS",
    "LAND_VAL", "IMPRVT_VAL", "LAST_YR_TX", "CALC_ACRE",
    "YR_CONSTR", "OWNER_NAME", "ST_ADDRESS", "CITY_STATE", "ZIP_CODE"
])

def fetch_parcels(county_name, offset=0, batch_size=2000):
    """
    Fetches a batch of parcels from the NJ ArcGIS REST API
    filtered by county name. Max 2000 records per request.
    """
    params = {
        "where":             f"COUNTY='{county_name}'",
        "outFields":         OUTFIELDS,
        "returnGeometry":    "false",
        "resultOffset":      offset,
        "resultRecordCount": batch_size,
        "f":                 "json"
    }
    try:
        response = requests.get(ARCGIS_API, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        if "error" in data:
            print(f"  API error: {data['error']}")
            return []
        return data.get("features", [])
    except requests.exceptions.RequestException as e:
        print(f"  Fetch error: {e}")
        return []

def filter_parcel(attrs):
    """
    Returns True if parcel meets criteria set in config.py.
    """
    class_code = str(attrs.get("PROP_CLASS", "")).strip()
    if class_code not in PROPERTY_CLASSES:
        return False
    land = float(attrs.get("LAND_VAL", 0) or 0)
    impr = float(attrs.get("IMPRVT_VAL", 0) or 0)
    if (land + impr) < MIN_ASSESSED_VALUE:
        return False
    return True

def insert_parcel(cur, attrs):
    """
    Inserts or updates a single parcel in the properties table.
    Safe to run multiple times without creating duplicates.
    """
    owner_address = " ".join(filter(None, [
        str(attrs.get("ST_ADDRESS", "")).strip(),
        str(attrs.get("CITY_STATE", "")).strip(),
        str(attrs.get("ZIP_CODE", "")).strip()
    ]))

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
            assessed_land     = EXCLUDED.assessed_land,
            assessed_building = EXCLUDED.assessed_building,
            last_tax_amount   = EXCLUDED.last_tax_amount,
            owner_name        = EXCLUDED.owner_name,
            owner_address     = EXCLUDED.owner_address,
            modiv_refreshed   = CURRENT_DATE
    """
    record = {
        "block":             str(attrs.get("PCLBLOCK", "")).strip(),
        "lot":               str(attrs.get("PCLLOT", "")).strip(),
        "muni_code":         str(attrs.get("PCL_MUN", "")).strip(),
        "muni_name":         str(attrs.get("MUN_NAME", "")).strip(),
        "county":            str(attrs.get("COUNTY", "")).strip(),
        "address":           str(attrs.get("PROP_LOC", "")).strip(),
        "class_code":        str(attrs.get("PROP_CLASS", "")).strip(),
        "assessed_land":     float(attrs.get("LAND_VAL", 0) or 0),
        "assessed_building": float(attrs.get("IMPRVT_VAL", 0) or 0),
        "last_tax_amount":   float(attrs.get("LAST_YR_TX", 0) or 0),
        "lot_acres":         float(attrs.get("CALC_ACRE", 0) or 0),
        "year_built":        int(attrs.get("YR_CONSTR", 0) or 0) or None,
        "owner_name":        str(attrs.get("OWNER_NAME", "")).strip(),
        "owner_address":     owner_address,
    }
    cur.execute(sql, record)

def ingest_county(county_name):
    """
    Pulls all qualifying parcels for a county and loads them
    into the properties table in batches of 2000.
    """
    print(f"\nStarting ingestion for {county_name} County...")
    conn = get_connection()
    cur = conn.cursor()

    total_inserted = 0
    total_skipped = 0
    offset = 0
    batch_size = 2000

    while True:
        print(f"  Fetching records {offset} to {offset + batch_size}...")
        features = fetch_parcels(county_name, offset, batch_size)

        if not features:
            print(f"  No more records at offset {offset}")
            break

        for feature in features:
            attrs = feature.get("attributes", {})
            if filter_parcel(attrs):
                try:
                    insert_parcel(cur, attrs)
                    total_inserted += 1
                except Exception as e:
                    print(f"  Insert error: {e}")
                    total_skipped += 1
            else:
                total_skipped += 1

        conn.commit()
        print(f"  Committed. Inserted/updated: {total_inserted} | Skipped: {total_skipped}")

        if len(features) < batch_size:
            break

        offset += batch_size

    cur.close()
    conn.close()
    print(f"\nCompleted {county_name} County.")
    print(f"Total inserted or updated: {total_inserted}")
    print(f"Total skipped: {total_skipped}")

def run_ingestion():
    """
    Runs ingestion for all counties in config.py.
    Add counties to config.py to expand coverage.
    """
    print("=== PREMARKET MOD-IV INGESTION ===")
    print(f"Counties: {COUNTIES}")
    print(f"Property classes: {PROPERTY_CLASSES}")
    for county in COUNTIES:
        ingest_county(county)
    print("\n=== INGESTION COMPLETE ===")

if __name__ == "__main__":
    run_ingestion()
