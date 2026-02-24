# /// script
# requires-python = ">=3.10"
# dependencies = ["boto3"]
# ///
"""Delete all objects under the latern/ prefix in S3."""

import os
import sys

import boto3


def human_size(nbytes: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(nbytes) < 1024:
            return f"{nbytes:.1f} {unit}"
        nbytes /= 1024
    return f"{nbytes:.1f} PB"


def main():
    bucket = os.environ.get("S3_BUCKET")
    endpoint = os.environ.get("S3_ENDPOINT")
    region = os.environ.get("S3_REGION")
    access_key = os.environ.get("S3_ACCESS_KEY")
    secret_key = os.environ.get("S3_SECRET_KEY")

    missing = []
    for name, val in [("S3_BUCKET", bucket), ("S3_ENDPOINT", endpoint),
                      ("S3_ACCESS_KEY", access_key), ("S3_SECRET_KEY", secret_key)]:
        if not val:
            missing.append(name)
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    prefix = "latern/"

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region or "us-east-1",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    # Collect all keys first
    keys = []
    total_size = 0
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
            total_size += obj["Size"]

    if not keys:
        print(f"No objects found under {prefix}")
        return

    print(f"Will delete {len(keys)} objects ({human_size(total_size)}) under {prefix}")
    answer = input("Confirm? [y/N] ").strip().lower()
    if answer != "y":
        print("Aborted.")
        return

    # Delete in batches of 1000 (S3 API limit)
    deleted = 0
    for i in range(0, len(keys), 1000):
        batch = [{"Key": k} for k in keys[i:i + 1000]]
        s3.delete_objects(Bucket=bucket, Delete={"Objects": batch})
        deleted += len(batch)
        print(f"  Deleted {deleted}/{len(keys)}")

    print(f"Done. Removed {len(keys)} objects ({human_size(total_size)}).")


if __name__ == "__main__":
    main()
