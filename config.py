# config.py
# Every setting for Premarket lives here.
# To change how the system behaves, edit this file only.

# ─── COUNTIES TO COVER ───────────────────────────────────────────────
# Add or remove NJ county names here to expand or shrink coverage
COUNTIES = [
    "Monmouth"
]

# ─── PROPERTY TYPES TO INCLUDE ───────────────────────────────────────
# MOD-IV class codes for residential, vacant land, and small commercial
# 2 = Residential, 1 = Vacant Land, 4A = Commercial
PROPERTY_CLASSES = ["2", "1", "4A"]

# ─── SIGNAL WEIGHTS ──────────────────────────────────────────────────
# How much each signal contributes to a property's score.
# Higher number = stronger indicator of motivation.
# Edit these as you learn what signals actually predict deals.
SIGNAL_WEIGHTS = {
    "TAX_D":    3.0,
    "LLC_I":    2.0,
    "PERMIT_X": 1.5,
    "CODE_V":   2.0,
    "LIEN":     2.5,
    "PROBATE":  3.0,
    "DEED_T":   1.0
}

# ─── TIME MULTIPLIER ─────────────────────────────────────────────────
# Score gets multiplied based on how long the oldest signal has been active.
# A property with signals for 6+ months scores significantly higher.
TIME_MULTIPLIER = {
    "under_30_days":   1.0,
    "30_to_90_days":   1.3,
    "90_to_180_days":  1.6,
    "over_180_days":   2.0
}

# ─── SURFACING THRESHOLD ─────────────────────────────────────────────
# Minimum score a property must reach before it appears in the dashboard.
# Raise this number to show fewer, higher quality results.
# Lower it to show more results with weaker signals.
SURFACE_THRESHOLD = 4.0

# ─── PIPELINE SCHEDULE ───────────────────────────────────────────────
# Which day of the week the pipeline runs automatically.
# Options: monday tuesday wednesday thursday friday saturday sunday
PIPELINE_RUN_DAY = "sunday"

# ─── EMAIL DIGEST ────────────────────────────────────────────────────
# Day and time the weekly digest email goes out to all users.
DIGEST_DAY = "sunday"
DIGEST_HOUR = 18

# ─── DATA SOURCES ────────────────────────────────────────────────────
# URLs for each NJ public data source the pipeline pulls from.
# If a government website changes its URL, update it here only.
DATA_SOURCES = {
    "nj_permits_api": "https://data.nj.gov/resource/w9se-dmra.json",
    "nj_business_registry": "https://njportal.com/dor/businessnamesearch",
    "nj_property_transparency": "https://nj.gov/transparency/property"
}

# ─── MINIMUM PROPERTY VALUE ──────────────────────────────────────────
# Filter out properties below this assessed value to avoid noise.
# Set to 0 to include all properties regardless of value.
MIN_ASSESSED_VALUE = 50000
