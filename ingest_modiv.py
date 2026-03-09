# ingest_modiv.py
# Pulls NJ parcel and MOD-IV data via the official NJ ArcGIS REST API.
# Uses the new 2025 endpoint. To add counties edit config.py only.

import requests
from db import get_connection
from config import COUNTIES, PROPERTY_CLASSES, MIN_ASSESSED_VALUE

# New ArcGIS REST API endpoint as of March 2026
ARCGIS_API = "https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/arcgis/rest/services/Parcels_and_MODIV_Composite_of_NJ/FeatureServer/0/query"

# Map NJ county names to their MOD-IV county codes
COUNTY_CODES = {
    "Atlantic":   "01",
    "Bergen":     "02",
    "Burlington": "03",
    "Camden":     "04",
    "Cape May":   "05",
    "Cumberland": "06",
    "Essex":      "07",
    "Gloucester": "08",
    "Hudson":     "09",
    "Hunterdon":  "10",
    "Mercer":     "11",
    "Middlesex":  "12",
    "Monmouth":   "13",
    "Morris":     "14",
    "Ocean":      "15",
    "Passaic":    "16",
    "Salem":      "17",
    "Somerset":   "18",
    "Sussex":     "19",
    "Union":      "20",
    "Warren":     "21"
}

def fetch_parcels(county_code, result_offset=0, result_record_count=1000):
    """
    Fetches a batch of parcels from the NJ ArcGIS REST API
    filtered by county code.
    """
    params = {
        "where":              f"COUNTY_CODE='{county_code}'",
        "outFields":          "PAMS_PIN,BLOCK,LOT,QUAL,MUN_CODE,MUN,COUNTY,PROPERTY_LOCATION,CLASS,LAND_VALUE,IMPR_VALUE,NET_TAXES,LOT_SIZE,YEAR_BUILT,OWNER_NAME,OWNER_ADDRESS",
        "returnGeometry":     "false",
        "resultOffset":       result_offset,
        "resultRecordCount":  result_record_count,
        "f":                  "json"
    }
    try:
        response = requests.get(ARCGIS_API, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get("features", [])
    except requests.exceptions.RequestException as e:
        print(f"  Fetch error: {e}")
        return []
    except Exception as e:
        print(f"  Parse error: {e}")
        return []

def filter_parcel(attrs):
    """
    Returns True if parcel meets config criteria.
    Edit thresholds in config.py only.
    """
    class_code = str(attrs.get("CLASS", "")).strip()
    if class_code not in PROPERTY_CLASSES:
        return False
    land = float(attrs.get("LAND_VALUE", 0) or 0)
    impr = float(attrs.get("IMPR_VALUE", 0) or 0)
    if (land + impr) < MIN_ASSESSED_VALUE:
        return False
    return True

def insert_parcel(cur, attrs):
    """
    Inserts or updates a single parcel into the properties table.
    Safe to run multiple times — updates existing records on conflict.
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
            assessed_land     = EXCLUDED.assessed_land,
            assessed_building = EXCLUDED.assessed_building,
            last_tax_amount   = EXCLUDED.last_tax_amount,
            owner_name        = EXCLUDED.owner_name,
            owner_address     = EXCLUDED.owner_address,
            modiv_refreshed   = CURRENT_DATE
    """
    record = {
        "block":             str(attrs.get("BLOCK", "")).strip(),
        "lot":               str(attrs.get("LOT", "")).strip(),
        "muni_code":         str(attrs.get("MUN_CODE", "")).strip(),
        "muni_name":         str(attrs.get("MUN", "")).strip(),
        "county":            str(attrs.get("COUNTY", "")).strip(),
        "address":           str(attrs.get("PROPERTY_LOCATION", "")).strip(),
        "class_code":        str(attrs.get("CLASS", "")).strip(),
        "assessed_land":     float(attrs.get("LAND_VALUE", 0) or 0),
        "assessed_building": float(attrs.get("IMPR_VALUE", 0) or 0),
        "last_tax_amount":   float(attrs.get("NET_TAXES", 0) or 0),
        "lot_acres":         float(attrs.get("LOT_SIZE", 0) or 0),
        "year_built":        int(attrs.get("YEAR_BUILT", 0) or 0) or None,
        "owner_name":        str(attrs.get("OWNER_NAME", "")).strip(),
        "owner_address":     str(attrs.get("OWNER_ADDRESS", "")).strip(),
    }
    cur.execute(sql, record)

def ingest_county(county_name):
    """
    Pulls all qualifying parcels for a county and loads them
    into the properties table in batches of 1000.
    """
    county_code = COUNTY_CODES.get(county_name)
    if not county_code:
        print(f"Unknown county: {county_name}. Check COUNTY_CODES in ingest_modiv.py")
        return

    print(f"\nStarting ingestion for {county_name} County (code: {county_code})...")
    conn = get_connection()
    cur = conn.cursor()

    total_inserted = 0
    total_skipped = 0
    offset = 0
    batch_size = 1000

    while True:
        print(f"  Fetching records {offset} to {offset + batch_size}...")
        features = fetch_parcels(county_code, offset, batch_size)

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
    Main entry point. Runs ingestion for all counties in config.py.
    Add counties to config.py to expand coverage — no other changes needed.
    """
    print("=== PREMARKET MOD-IV INGESTION ===")
    print(f"Counties: {COUNTIES}")
    print(f"Property classes: {PROPERTY_CLASSES}")
    for county in COUNTIES:
        ingest_county(county)
    print("\n=== INGESTION COMPLETE ===")

if __name__ == "__main__":
    run_ingestion()
