# config.py
# Central configuration for the Premarket pipeline.
# All signal sources, scoring weights, and system settings live here.
# Change values here without touching any other file.

# -------------------------
# COUNTIES IN SCOPE
# -------------------------
COUNTIES = ["Monmouth"]

# Future: ["Monmouth", "Ocean", "Middlesex"]

# -------------------------
# PROPERTY CLASSES TO TRACK
# -------------------------
# 1 = Vacant Land
# 2 = Residential
# 4A = Commercial
PROPERTY_CLASSES = ["1", "2", "4A"]

# -------------------------
# SCORING SETTINGS
# -------------------------

# Number of days before a property is marked stale
STALE_THRESHOLD_DAYS = 90

# Recency multipliers — more recent signals score higher
RECENCY_MULTIPLIERS = {
    "0_30":   1.0,   # detected within 30 days
    "31_90":  1.3,   # detected 31-90 days ago
    "91_180": 1.6,   # detected 91-180 days ago
    "181_plus": 2.0  # detected over 180 days ago — long standing distress
}

# -------------------------
# SIGNAL HIERARCHY
# -------------------------

# Entry point signals — strong enough to surface a property alone.
# These query government sources directly and create new property
# records when distress is detected.
ENTRY_POINT_SIGNALS = [
    "TAX_D",       # tax delinquency
    "PROBATE",     # probate filing on owner
    "LIEN",        # lien recorded against title
    "VACANCY",     # municipal vacant property registry
    "FORECLOSURE"  # lis pendens / foreclosure filing
]

# Corroborating signals — add weight to already flagged properties.
# Never create new property records on their own.
# Run only against properties already in the database.
# Can be triggered manually via Deep Scan on any property.
CORROBORATING_SIGNALS = [
    "PERMIT_X",  # no permit activity for 10+ years
    "LLC_I",     # inactive or dissolved LLC ownership
    "CODE_V",    # active code violation
    "DEED_T"     # unusual deed transfer pattern
]

# Future signals — planned but not yet built
# TAX_SALE — tax lien already sold at municipal tax sale

# -------------------------
# SCORING SETTINGS
# -------------------------

STALE_THRESHOLD_DAYS = 90

RECENCY_MULTIPLIERS = {
    "0_30":     1.0,
    "31_90":    1.3,
    "91_180":   1.6,
    "181_plus": 2.0
}

SIGNAL_WEIGHTS = {
    "TAX_D":       3.0,
    "PROBATE":     3.0,
    "LIEN":        2.5,
    "VACANCY":     3.0,
    "FORECLOSURE": 3.5,
    "CODE_V":      2.0,
    "LLC_I":       2.0,
    "PERMIT_X":    1.5,
    "DEED_T":      1.0
}

SURFACE_THRESHOLD = 1.0

# -------------------------
# SIGNAL SOURCES
# -------------------------

PERMITS_API = "https://data.nj.gov/resource/w9se-dmra.json"
PERMITS_INACTIVITY_YEARS = 10

LLC_API = "https://data.nj.gov/resource/p4ys-idjb.json"

MONMOUTH_OPRS = "https://oprs.co.monmouth.nj.us/Oprs/taxboard/tbindex.aspx"

ARCGIS_API = "https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/arcgis/rest/services/Parcels_Composite_NJ_WM/FeatureServer/0/query"

# NJ courts — foreclosure / lis pendens filings
NJ_COURTS_API = "https://portal.njcourts.gov"

# NJ municipal vacant property registries — varies by municipality
# Populated per municipality as sources are confirmed
VACANCY_SOURCES = {}

# -------------------------
# SCAN FREQUENCIES
# -------------------------

SCAN_FREQUENCIES = {
    "TAX_D":       "quarterly",
    "PROBATE":     "weekly",
    "LIEN":        "weekly",
    "VACANCY":     "weekly",
    "FORECLOSURE": "weekly",
    "LLC_I":       "daily",
    "PERMIT_X":    "weekly",
    "CODE_V":      "weekly",
    "DEED_T":      "weekly"
}

TAX_DUE_DATES = [
    (2, 1),
    (5, 1),
    (8, 1),
    (11, 1)
]

# -------------------------
# GITHUB ACTIONS SETTINGS
# -------------------------

BATCH_SIZE = 500

# -------------------------
# NOTIFICATION SETTINGS
# -------------------------

STALE_ALERT_DAYS = 90
DIGEST_DAY = "monday"
