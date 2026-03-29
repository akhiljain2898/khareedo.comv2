"""
scripts/test_pipeline.py
Runs the full worker pipeline against a real product query.
Use this to verify Serper + Firecrawl + Haiku all work end-to-end
BEFORE building the payment flow on top.

Usage:
    python scripts/test_pipeline.py "hydraulic pump"
    python scripts/test_pipeline.py "wheat gluten"
"""

import sys
import os
import json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from worker.pipeline import run_pipeline

def main():
    query = sys.argv[1] if len(sys.argv) > 1 else "safety gloves"
    print(f"\nRunning pipeline for: '{query}'")
    print("=" * 60)

    results, keywords_used = run_pipeline(query)

    print("=" * 60)
    print(f"\nDone.")
    print(f"  Results:       {len(results)}")
    print(f"  Keywords used: {keywords_used}")

    if results:
        print(f"\nFirst result:")
        print(json.dumps(results[0], indent=2))
        print(f"\nAll names found:")
        for r in results:
            print(f"  - {r.get('name')} | {r.get('phone')} | {r.get('email')}")
    else:
        print("\nNo valid results found for this query.")
        print("Check your SERPER_API_KEY, FIRECRAWL_API_KEY, and ANTHROPIC_API_KEY.")

if __name__ == "__main__":
    main()
