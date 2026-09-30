"""The S3 store: the sink's files in SeaweedFS."""

import boto3

from ecommerce.core.config import BUCKET, S3, S3_PREFIX


def delete_files() -> int:
    """
    Deletes the files the S3 sink wrote under this project's prefix.

    Returns:
        int: How many files were deleted.
    """
    bucket = boto3.resource("s3", **S3).Bucket(BUCKET)
    deleted = bucket.objects.filter(Prefix=S3_PREFIX).delete()
    return sum(len(r.get("Deleted", [])) for r in deleted)
