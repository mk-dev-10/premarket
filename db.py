import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_connection():
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise Exception("DATABASE_URL environment variable not set")
    conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
    return conn

def test_connection():
    try:
        conn = get_connection()
        cur = conn.cursor()
        tables = ['properties', 'signals', 'scores', 'watchlists']
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()
            print(f"Table {table} exists with {count['count']} rows")
        cur.close()
        conn.close()
        print("Database connection successful")
    except Exception as e:
        print(f"Connection failed: {e}")

test_connection()
