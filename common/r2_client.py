"""
common/r2_client.py
Cloudflare R2 via boto3 (S3-compatible API).
Handles upload, presigned URL generation, and deletion.
"""

import boto3
from botocore.config import Config
from common.config import (
    R2_ACCESS_KEY_ID,
    R2_SECRET_ACCESS_KEY,
    R2_ENDPOINT_URL,
    R2_BUCKET_NAME,
)


def get_client():
    """Return a boto3 S3 client pointed at Cloudflare R2."""
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT_URL,
        aws_access_key_id=R2_ACCESS_KEY_ID,
        aws_secret_access_key=R2_SECRET_ACCESS_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto",
    )


def upload_bytes(key: str, data: bytes, content_type: str = "application/octet-stream"):
    """
    Upload raw bytes to R2 at the given key.
    R2 TTL / lifecycle rules are set at the bucket level in Cloudflare dashboard,
    not per-object — see architecture doc for TTL config instructions.
    """
    client = get_client()
    client.put_object(
        Bucket=R2_BUCKET_NAME,
        Key=key,
        Body=data,
        ContentType=content_type,
    )


def get_bytes(key: str) -> bytes:
    """
    Retrieve raw bytes from R2 by key.
    Raises botocore.exceptions.ClientError if key doesn't exist.
    """
    client = get_client()
    response = client.get_object(Bucket=R2_BUCKET_NAME, Key=key)
    return response["Body"].read()


def delete_object(key: str):
    """
    Delete an object from R2 by key.
    Called after the customer downloads — removes the download/ key.
    The archive/ key is left intact (deleted by R2 lifecycle rule after 7 days).
    """
    client = get_client()
    client.delete_object(Bucket=R2_BUCKET_NAME, Key=key)


def object_exists(key: str) -> bool:
    """Check if an object exists in R2 without downloading it."""
    client = get_client()
    try:
        client.head_object(Bucket=R2_BUCKET_NAME, Key=key)
        return True
    except Exception:
        return False
