# scorer.py
# Calculates scores for all properties with active signals.
# Score = sum of signal weights * recency multipliers.
# Higher score = more signals + more recent detection.
# Properties with no active signals are not scored.
# Properties inactive for 90+ days are marked stale.

from db import get_connection
from config import SIGNAL_WEIGHTS, RECENCY_MULTIPLIERS, STALE_THRESHOLD_DAYS, SURFACE_THRESHOLD
from datetime import date, datetime
import psycopg2.extras

def get_recency_multiplier(detected_date):
    """
    Returns the recency multiplier for a signal based on
    how many days ago it was detected.
    Older signals score higher — long standing distress
    means a more motivated seller.
    """
    if isinstance(detected_date, str):
        detected_date = datetime.strptime(detected_date, "%Y-%m-%d").date()

    days_ago = (date.today() - detected_date).days

    if days_ago <= 30:
        return RECENCY_MULTIPLIERS["0_30"]
    elif days_ago <= 90:
        return RECENCY_MULTIPLIERS["31_90"]
    elif days_ago <= 180:
        return RECENCY_MULTIPLIERS["91_180"]
    else:
        return RECENCY_MULTIPLIERS["181_plus"]

def calculate_score(signals):
    """
    Takes a list of active signals for a single property
    and returns the total recency weighted score.
    """
    total = 0.0
    for signal in signals:
        weight = SIGNAL_WEIGHTS.get(signal["signal_code"], 1.0)
        multiplier = get_recency_multiplier(signal["detected_date"])
        total += weight * multiplier
    return round(total, 2)

def is_stale(signals):
    """
    Returns True if the most recent signal on this property
    is older than STALE_THRESHOLD_DAYS.
    Stale properties get visually deprioritized on the frontend.
    """
    if not signals:
        return True
    most_recent = max(signals, key=lambda s: s["detected_date"])
    detected = most_recent["detected_date"]
    if isinstance(detected, str):
        detected = datetime.strptime(detected, "%Y-%m-%d").date()
    days_ago = (date.today() - detected).days
    return days_ago > STALE_THRESHOLD_DAYS

def get_properties_with_signals(conn):
    """
    Returns all properties that have at least one active signal.
    Only these properties get scored.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT DISTINCT p.property_id
        FROM properties p
        INNER JOIN signals s ON s.property_id = p.property_id
        WHERE s.status = 'ACTIVE'
    """)
    results = cur.fetchall()
    cur.close()
    return [r["property_id"] for r in results]

def get_active_signals_for_property(conn, property_id):
    """
    Returns all active signals for a single property.
    """
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("""
        SELECT signal_code, detected_date
        FROM signals
        WHERE property_id = %s
        AND status = 'ACTIVE'
    """, (property_id,))
    results = cur.fetchall()
    cur.close()
    return results

def upsert_score(conn, property_id, signal_count, recency_score, stale):
    """
    Inserts or updates the score for a property.
    If a score row already exists it gets overwritten.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO scores (
            property_id, signal_count, recency_score,
            is_stale, last_calculated, first_surfaced
        )
        VALUES (%s, %s, %s, %s, NOW(), CURRENT_DATE)
        ON CONFLICT (property_id)
        DO UPDATE SET
            signal_count    = EXCLUDED.signal_count,
            recency_score   = EXCLUDED.recency_score,
            is_stale        = EXCLUDED.is_stale,
            last_calculated = NOW()
    """, (property_id, signal_count, recency_score, stale))
    conn.commit()
    cur.close()

def run_scorer():
    """
    Main entry point. Scores every property with active signals.
    Marks stale properties. Prints summary on completion.
    """
    print("=== PREMARKET SCORER ===")
    conn = get_connection()

    property_ids = get_properties_with_signals(conn)
    print(f"Properties with active signals: {len(property_ids)}")

    scored = 0
    stale_count = 0
    surfaced = 0

    for property_id in property_ids:
        signals = get_active_signals_for_property(conn, property_id)
        score = calculate_score(signals)
        stale = is_stale(signals)
        signal_count = len(signals)

        if stale:
            stale_count += 1

        if score >= SURFACE_THRESHOLD:
            surfaced += 1

        upsert_score(conn, property_id, signal_count, score, stale)
        scored += 1

    conn.close()
    print(f"Properties scored:   {scored}")
    print(f"Surfaced (>= {SURFACE_THRESHOLD}): {surfaced}")
    print(f"Marked stale:        {stale_count}")
    print("=== SCORING COMPLETE ===")

if __name__ == "__main__":
    run_scorer()
