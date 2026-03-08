import os
import psycopg2
from psycopg2.extras import RealDictCursor

def get_connection():
    """
    Creates and returns a connection to your Supabase PostgreSQL database.
    Every other file in the pipeline imports this function instead of 
    managing connections individually.
    """
    database_url = os.environ.get("DATABASE_URL")
    
    if not database_url:
        raise Exception("DATABASE_URL environment variable not set")
    
    conn = psycopg2.connect(database_url, cursor_factory=RealDictCursor)
    return conn

def test_connection():
    """
    Tests that the database connection works and all four tables exist.
    Run this first to confirm everything is wired up correctly.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # Check all four tables exist
        tables = ['properties', 'signals', 'scores', 'watchlists']
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()
            print(f"✓ Table '{table}' exists — {count['count']} rows")
        
        cur.close()
        conn.close()
        print("\n✓ Database connection successful. All tables verified.")
        
    except Exception as e:
        print(f"✗ Connection failed: {e}")

if __name__ == "__main__":
    test_connection()
```

Commit that file the same way.

**Step 5 — Create one more file**

Name it exactly:
```
Procfile
```

Paste this single line:
```
worker: python db.py
