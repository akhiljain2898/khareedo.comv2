"""
scripts/test_r2.py
Verifies Cloudflare R2 by uploading a test file and reading it back.

Usage:
    python scripts/test_r2.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.r2_client import upload_bytes, get_bytes, delete_object, object_exists

TEST_KEY  = "test/setup_check.txt"
TEST_DATA = b"khareedo r2 connection test"

def main():
    print("Testing Cloudflare R2...")

    print("  Uploading test file...")
    try:
        upload_bytes(TEST_KEY, TEST_DATA, content_type="text/plain")
        print("  ✓ Upload succeeded")
    except Exception as e:
        print(f"  ✗ Upload failed: {e}")
        sys.exit(1)

    print("  Checking existence...")
    try:
        exists = object_exists(TEST_KEY)
        assert exists, "object_exists returned False after upload"
        print("  ✓ object_exists works")
    except Exception as e:
        print(f"  ✗ Existence check failed: {e}")
        sys.exit(1)

    print("  Fetching file...")
    try:
        data = get_bytes(TEST_KEY)
        assert data == TEST_DATA, f"Data mismatch: {data}"
        print("  ✓ Fetch succeeded and data matches")
    except Exception as e:
        print(f"  ✗ Fetch failed: {e}")
        sys.exit(1)

    print("  Deleting test file...")
    try:
        delete_object(TEST_KEY)
        exists = object_exists(TEST_KEY)
        assert not exists, "File still exists after delete"
        print("  ✓ Delete succeeded")
    except Exception as e:
        print(f"  ✗ Delete failed: {e}")
        sys.exit(1)

    print("\nAll done. R2 is ready.")

if __name__ == "__main__":
    main()
