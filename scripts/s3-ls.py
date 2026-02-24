# /// script
# requires-python = ">=3.10"
# dependencies = ["boto3"]
# ///
"""Show S3 bucket contents grouped by subfolder with file counts and sizes."""

import os
import sys
from collections import defaultdict

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
    prefix = os.environ.get("S3_PREFIX", "").strip("/")

    missing = []
    for name, val in [("S3_BUCKET", bucket), ("S3_ENDPOINT", endpoint),
                      ("S3_ACCESS_KEY", access_key), ("S3_SECRET_KEY", secret_key)]:
        if not val:
            missing.append(name)
    if missing:
        print(f"Missing env vars: {', '.join(missing)}", file=sys.stderr)
        sys.exit(1)

    if prefix:
        prefix += "/"

    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=region or "us-east-1",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )

    # Collect per-folder stats
    folders: dict[str, dict] = defaultdict(lambda: {"count": 0, "size": 0})
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            # Derive subfolder relative to prefix
            rel = key[len(prefix):]
            parts = rel.split("/")
            folder = parts[0] + "/" if len(parts) > 1 else "(root)"
            folders[folder]["count"] += 1
            folders[folder]["size"] += obj["Size"]

    print(f"S3 Bucket: {bucket}")
    print(f"Prefix:    {prefix or '(none)'}")
    print()

    total_count = 0
    total_size = 0

    for folder in sorted(folders):
        stats = folders[folder]
        label = f"{prefix}{folder}" if folder != "(root)" else f"{prefix}(root files)"
        print(f"{label}")
        print(f"  Files: {stats['count']:<6}Size: {human_size(stats['size'])}")
        total_count += stats["count"]
        total_size += stats["size"]

    if not folders:
        print("  (empty)")

    print()
    print(f"Total: {total_count} files, {human_size(total_size)}")


if __name__ == "__main__":
    main()
