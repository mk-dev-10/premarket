# ingest_modiv.py
# Pulls NJ parcel and MOD-IV data via the official NJ ArcGIS REST API.
# Queries by municipality to avoid county level record caps.
# To add counties: add municipality codes below and update config.py.
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

# Official NJ Division of Taxation municipality codes
# Source: nj.gov/treasury/taxation/pdf/lpt/cntycode.pdf
# To add a new county add its municipalities here and in config.py

COUNTY_MUNICIPALITIES = {
    "Monmouth": [
        "1301",  # ABERDEEN TWP
        "1302",  # ALLENHURST BORO
        "1303",  # ALLENTOWN BORO
        "1304",  # ASBURY PARK CITY
        "1305",  # ATLANTIC HIGHLANDS BORO
        "1306",  # AVON BY THE SEA BORO
        "1307",  # BELMAR BORO
        "1308",  # BRADLEY BEACH BORO
        "1309",  # BRIELLE BORO
        "1310",  # COLTS NECK TWP
        "1311",  # DEAL BORO
        "1312",  # EATONTOWN BORO
        "1313",  # ENGLISHTOWN BORO
        "1314",  # FAIR HAVEN BORO
        "1315",  # FARMINGDALE BORO
        "1316",  # FREEHOLD BORO
        "1317",  # FREEHOLD TWP
        "1318",  # HAZLET TWP
        "1319",  # HIGHLANDS BORO
        "1320",  # HOLMDEL TWP
        "1321",  # HOWELL TWP
        "1322",  # INTERLAKEN BORO
        "1323",  # KEANSBURG BORO
        "1324",  # KEYPORT BORO
        "1325",  # LITTLE SILVER BORO
        "1326",  # LOCH ARBOUR VILLAGE
        "1327",  # LONG BRANCH CITY
        "1328",  # MANALAPAN TWP
        "1329",  # MANASQUAN BORO
        "1330",  # MARLBORO TWP
        "1331",  # MATAWAN BORO
        "1332",  # MIDDLETOWN TWP
        "1333",  # MILLSTONE TWP
        "1334",  # MONMOUTH BEACH BORO
        "1335",  # NEPTUNE TWP
        "1336",  # NEPTUNE CITY BORO
        "1337",  # OCEAN TWP
        "1338",  # OCEANPORT BORO
        "1339",  # RED BANK BORO
        "1340",  # ROOSEVELT BORO
        "1341",  # RUMSON BORO
        "1342",  # SEA BRIGHT BORO
        "1343",  # SEA GIRT BORO
        "1344",  # SHREWSBURY BORO
        "1345",  # SHREWSBURY TWP
        "1346",  # LAKE COMO BORO
        "1347",  # SPRING LAKE BORO
        "1348",  # SPRING LAKE HEIGHTS BORO
        "1349",  # TINTON FALLS BORO
        "1350",  # UNION BEACH BORO
        "1351",  # UPPER FREEHOLD TWP
        "1352",  # WALL TWP
        "1353",  # WEST LONG BRANCH BORO
    ]
}

def fetch_parcels_by_muni(muni_code, offset=0, batch_size=2000):
    """
    Fetches a batch of parcels for a single municipality.
    Querying by municipality avoids county level record caps.
    """
    params = {
        "where":             f"PCL_MUN='{muni_code}'",
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

def ingest_municipality(conn, muni_code):
    """
    Pulls all qualifying parcels for a single municipality.
    Returns count of inserted and skipped records.
    """
    cur = conn.cursor()
    inserted = 0
    skipped = 0
    offset = 0
    batch_size = 2000

    while True:
        features = fetch_parcels_by_muni(muni_code, offset, batch_size)

        if not features:
            break

        for feature in features:
            attrs = feature.get("attributes", {})
            if filter_parcel(attrs):
                try:
                    insert_parcel(cur, attrs)
                    inserted += 1
                except Exception as e:
                    print(f"    Insert error: {e}")
                    skipped += 1
            else:
                skipped += 1

        conn.commit()

        if len(features) < batch_size:
            break

        offset += batch_size

    cur.close()
    return inserted, skipped

def ingest_county(county_name):
    """
    Pulls all qualifying parcels for a county by iterating
    through each municipality individually to avoid record caps.
    """
    municipalities = COUNTY_MUNICIPALITIES.get(county_name, [])
    if not municipalities:
        print(f"No municipalities found for {county_name}. Check COUNTY_MUNICIPALITIES in ingest_modiv.py")
        return

    print(f"\nStarting ingestion for {county_name} County...")
    print(f"Municipalities to process: {len(municipalities)}")

    conn = get_connection()
    total_inserted = 0
    total_skipped = 0

    for i, muni_code in enumerate(municipalities):
        print(f"  [{i+1}/{len(municipalities)}] Municipality {muni_code}...", end=" ")
        inserted, skipped = ingest_municipality(conn, muni_code)
        total_inserted += inserted
        total_skipped += skipped
        print(f"inserted: {inserted} | skipped: {skipped}")

    conn.close()
    print(f"\nCompleted {county_name} County.")
    print(f"Total inserted or updated: {total_inserted}")
    print(f"Total skipped: {total_skipped}")

def run_ingestion():
    """
    Main entry point. Runs ingestion for all counties in config.py.
    Add counties to config.py and municipality codes above to expand.
    """
    print("=== PREMARKET MOD-IV INGESTION ===")
    print(f"Counties: {COUNTIES}")
    print(f"Property classes: {PROPERTY_CLASSES}")
    for county in COUNTIES:
        ingest_county(county)
    print("\n=== INGESTION COMPLETE ===")

if __name__ == "__main__":
    run_ingestion()
