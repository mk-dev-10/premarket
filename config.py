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

# Signal weights — how much each signal contributes to the score
# These are starting values to be tuned during testing
SIGNAL_WEIGHTS = {
    "TAX_D":   3.0,  # tax delinquency — strongest motivated seller indicator
    "PROBATE": 3.0,  # probate — heirs often want quick liquidation
    "LIEN":    2.5,  # lien on title — complicates sale, motivates resolution
    "CODE_V":  2.0,  # code violation — distressed or absentee owner
    "LLC_I":   2.0,  # inactive LLC — ownership in limbo
    "PERMIT_X": 1.5, # permit inactivity — weak alone, meaningful combined
    "DEED_T":  1.0   # unusual deed activity — watch signal, low weight alone
}

# Minimum score to surface a property to users
# Set low during testing so we can see output
SURFACE_THRESHOLD = 1.0

# -------------------------
# SIGNAL SOURCES
# -------------------------

# NJ Building Permits — Socrata API
PERMITS_API = "https://data.nj.gov/resource/w9se-dmra.json"
PERMITS_INACTIVITY_YEARS = 10  # flag if no permits in this many years

# NJ Business Registry — for LLC status checks
LLC_API = "https://data.nj.gov/resource/p4ys-idjb.json"

# Monmouth County OPRS — for owner enrichment on flagged properties
MONMOUTH_OPRS = "https://oprs.co.monmouth.nj.us/Oprs/taxboard/tbindex.aspx"

# NJ ArcGIS Parcels — used for property lookups by block/lot
ARCGIS_API = "https://services2.arcgis.com/XVOqAjTOJ5P6ngMu/arcgis/rest/services/Parcels_Composite_NJ_WM/FeatureServer/0/query"

# -------------------------
# SCAN FREQUENCIES
# -------------------------
# These map to GitHub Actions cron schedules
# Reference only — actual schedules set in .github/workflows/

SCAN_FREQUENCIES = {
    "TAX_D":   "quarterly",  # run after each NJ tax due date
    "PROBATE": "weekly",
    "LIEN":    "weekly",
    "CODE_V":  "weekly",
    "LLC_I":   "daily",      # NJ registry updates in real time
    "PERMIT_X": "weekly",
    "DEED_T":  "weekly"
}

# NJ property tax due dates (month, day)
TAX_DUE_DATES = [
    (2, 1),   # February 1
    (5, 1),   # May 1
    (8, 1),   # August 1
    (11, 1)   # November 1
]

# -------------------------
# GITHUB ACTIONS SETTINGS
# -------------------------
# How many properties to process per signal checker run
# Keeps individual runs fast and within Actions time limits
BATCH_SIZE = 500

# -------------------------
# NOTIFICATION SETTINGS
# -------------------------
STALE_ALERT_DAYS = 90       # days before marking property stale
DIGEST_DAY = "monday"       # day of week for weekly digest email
