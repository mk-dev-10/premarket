# scorer.py
# Reads active signals from the signals table and calculates
# a weighted score for each property. Writes results to scores table.
# To change signal weights or time multipliers edit config.py only.

from db import get_connection
from config import SIGNAL_WEIGHTS, TIME_MULTIPLIER, SURFACE_THRESHOLD
from datetime import date

def get_time_multiplier(days_active):
    """
    Returns a score multiplier based on how long the oldest
    active signal has been present. Longer = higher multiplier.
    Thresholds defined in config.py.
    """
    if days_active < 30:
        return TIME_MULTIPLIER["under_30_days"]
    elif days_active < 90:
        return TIME_MULTIPLIER["30_to_90_days"]
    elif days_active < 180:
        return TIME_MULTIPLIER["90_to_180_days"]
    else:
        return TIME_MULTIPLIER["over_180_days"]

def calculate_score(signals):
    """
    Given a list of active signal rows for one property,
    calculates the final weighted score with time multiplier applied.
    """
    if not signals:
        return 0.0, 0, 0

    base_score = 0.0
    signal_count = len(signals)
    today = date.today()
    max_days = 0

    for signal in signals:
        code = signal["signal_code"]
        weight = SIGNAL_WEIGHTS.get(code, 1.0)
        base_score += weight

        detected = signal["detected_date"]
        if detected:
            days = (today - detected).days
            if days > max_days:
                max_days = days

    multiplier = get_time_multiplier(max_days)
    final_score = round(base_score * multiplier, 2)

    return final_score, signal_count, max_days

def get_properties_with_signals(conn):
    """
    Returns all property IDs that have at least one active signal.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT DISTINCT property_id 
        FROM signals 
        WHERE status = 'ACTIVE'
    """)
    rows = cur.fetchall()
    cur.close()
    return [row["property_id"] for row in rows]

def get_active_signals_for_property(conn, property_id):
    """
    Returns all active signal rows for a given property.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT signal_code, detected_date 
        FROM signals 
        WHERE property_id = %s AND status = 'ACTIVE'
    """, (property_id,))
    rows = cur.fetchall()
    cur.close()
    return rows

def upsert_score(conn, property_id, score, signal_count, oldest_days, surfaced, first_surfaced):
    """
    Inserts or updates the score record for a property.
    Updates every field on conflict since scores change weekly.
    """
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO scores (
            property_id, current_score, signal_count,
            oldest_signal_days, last_calculated,
            surfaced, first_surfaced
        )
        VALUES (
            %s, %s, %s, %s, NOW(), %s, %s
        )
        ON CONFLICT (property_id)
        DO UPDATE SET
            current_score      = EXCLUDED.current_score,
            signal_count       = EXCLUDED.signal_count,
            oldest_signal_days = EXCLUDED.oldest_signal_days,
            last_calculated    = NOW(),
            surfaced           = EXCLUDED.surfaced,
            first_surfaced     = CASE 
                WHEN scores.first_surfaced IS NULL AND EXCLUDED.surfaced = TRUE 
                THEN CURRENT_DATE 
                ELSE scores.first_surfaced 
            END
    """, (property_id, score, signal_count, oldest_days, surfaced, first_surfaced))
    conn.commit()
    cur.close()

def run_scorer():
    """
    Main entry point. Scores all properties with active signals.
    Runs after every signal detection cycle.
    """
    print("=== PREMARKET SCORER ===")
    conn = get_connection()

    property_ids = get_properties_with_signals(conn)
    print(f"Properties with active signals: {len(property_ids)}")

    scored = 0
    surfaced = 0

    for property_id in property_ids:
        signals = get_active_signals_for_property(conn, property_id)
        score, signal_count, oldest_days = calculate_score(signals)

        is_surfaced = score >= SURFACE_THRESHOLD

        # Preserve existing first_surfaced date if already set
        cur = conn.cursor()
        cur.execute("SELECT first_surfaced FROM scores WHERE property_id = %s", (property_id,))
        existing = cur.fetchone()
        cur.close()

        first_surfaced = None
        if existing and existing["first_surfaced"]:
            first_surfaced = existing["first_surfaced"]
        elif is_surfaced:
            first_surfaced = date.today()

        upsert_score(conn, property_id, score, signal_count, oldest_days, is_surfaced, first_surfaced)
        scored += 1
        if is_surfaced:
            surfaced += 1

    conn.close()
    print(f"Properties scored: {scored}")
    print(f"Properties surfaced above threshold: {surfaced}")
    print("=== SCORING COMPLETE ===")

if __name__ == "__main__":
    run_scorer()
