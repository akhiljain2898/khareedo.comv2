"""
scripts/setup_sheets.py
Creates the Google Sheet with both tabs (Jobs Log + Downloads Log)
if they don't already exist.
Prints the Sheet URL at the end — send this to the founder.

Usage:
    python scripts/setup_sheets.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from worker.sheets_log import setup_sheet

def main():
    print("Setting up Google Sheets ops log...")
    try:
        url = setup_sheet()
        print(f"\n✓ Sheet is ready.")
        print(f"\n  Share this URL with the founder:")
        print(f"  {url}")
        print(f"\n  The sheet has two tabs:")
        print(f"  - 'Jobs Log'      — updated by worker on every job completion")
        print(f"  - 'Downloads Log' — updated by API on every download")
        print(f"\n  Set up conditional formatting in Google Sheets:")
        print(f"  Jobs Log: color rows red when Job Status = 'failed'")
        print(f"  Jobs Log: color rows amber when Job Status = 'partial'")
    except Exception as e:
        print(f"\n✗ Sheet setup failed: {e}")
        print("\nCommon causes:")
        print("  - GOOGLE_SERVICE_ACCOUNT_JSON not set or malformed")
        print("  - GOOGLE_SHEET_ID not set (create a blank Sheet first and copy its ID from the URL)")
        print("  - Service account not granted Editor access to the Sheet")
        sys.exit(1)

if __name__ == "__main__":
    main()
