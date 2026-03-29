"""
scripts/setup_db.py
Run once to create the transactions table in Postgres.
Safe to run multiple times.

Usage:
    python scripts/setup_db.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db import create_tables, get_connection

def main():
    print("Testing Postgres connection...")
    try:
        conn = get_connection()
        conn.close()
        print("✓ Connected to Postgres")
    except Exception as e:
        print(f"✗ Connection failed: {e}")
        sys.exit(1)

    print("Creating tables...")
    try:
        create_tables()
        print("✓ transactions table ready")
    except Exception as e:
        print(f"✗ Table creation failed: {e}")
        sys.exit(1)

    print("\nAll done. Postgres is ready.")

if __name__ == "__main__":
    main()
