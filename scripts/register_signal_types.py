#!/usr/bin/env python3
"""
One-time script to register new signal types in the shared database.

Usage:
  python scripts/register_signal_types.py

Idempotent — safe to run multiple times (uses ON CONFLICT DO NOTHING).
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from src.db import get_db_connection
from src.signals import register_signal_types, SIGNAL_TYPES


def main():
    print("Connecting to database...")
    conn = get_db_connection()

    print(f"Registering {len(SIGNAL_TYPES)} signal types...")
    registered = register_signal_types(conn)

    print("\nRegistered signal types:")
    print("-" * 60)
    for code, uuid in registered.items():
        config = SIGNAL_TYPES[code]
        print(f"  {code:30s} weight={config['base_weight']}  id={uuid}")

    print(f"\nTotal: {len(registered)} signal types registered.")

    # Quick verification: count existing signal types
    from psycopg2.extras import RealDictCursor
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute("SELECT COUNT(*) as count FROM signal_types WHERE is_active = TRUE")
        total = cur.fetchone()["count"]
        print(f"Total active signal types in DB: {total}")

    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
