"""The SeaweedFS buckets the other stores keep their files in, through its S3 API."""

import os

import boto3


def client():
    """
    Returns an S3 client for SeaweedFS, with the settings `airq.core.config` gives PyIceberg.

    Returns:
        botocore.client.S3: The client.
    """
    return boto3.client(
        "s3",
        endpoint_url=os.environ["PYICEBERG_CATALOG__ODCTL__S3__ENDPOINT"],
        aws_access_key_id=os.environ["PYICEBERG_CATALOG__ODCTL__S3__ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ[
            "PYICEBERG_CATALOG__ODCTL__S3__SECRET_ACCESS_KEY"
        ],
        region_name=os.environ["PYICEBERG_CATALOG__ODCTL__S3__REGION"],
    )


def keys(s3, bucket: str, prefix: str) -> list[str]:
    """
    Lists every object key under a prefix.

    Args:
        s3 (botocore.client.S3): The S3 client.
        bucket (str): The bucket.
        prefix (str): The key prefix.

    Returns:
        list[str]: The keys, across every page of the listing.
    """
    pages = s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for page in pages for o in page.get("Contents", [])]


def delete(s3, bucket: str, keys: list[str]) -> int:
    """
    Deletes objects, a thousand at a time, which is the most one request takes.

    Args:
        s3 (botocore.client.S3): The S3 client.
        bucket (str): The bucket.
        keys (list[str]): The keys to delete.

    Returns:
        int: How many objects were deleted.
    """
    for i in range(0, len(keys), 1000):
        batch = [{"Key": k} for k in keys[i : i + 1000]]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch, "Quiet": True})
    return len(keys)
