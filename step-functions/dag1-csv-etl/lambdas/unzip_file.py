"""
Lambda: UnzipFile
Downloads a ZIP file from S3, extracts CSVs, writes them back to S3.
Returns a list of S3 keys for the extracted CSVs.
"""

import io
import json
import zipfile

import boto3


s3 = boto3.client("s3")


def handler(event, context):
    bucket = event["s3_bucket"]
    zip_key = event["zip_key"]
    extract_prefix = event.get("extract_prefix", "extracted/")

    # Download the ZIP file from S3
    zip_obj = s3.get_object(Bucket=bucket, Key=zip_key)
    zip_bytes = io.BytesIO(zip_obj["Body"].read())

    csv_keys = []

    with zipfile.ZipFile(zip_bytes, "r") as zf:
        for filename in zf.namelist():
            if not filename.endswith(".csv"):
                continue

            # Read file content and upload to S3
            csv_data = zf.read(filename)
            dest_key = f"{extract_prefix}{filename}"
            s3.put_object(Bucket=bucket, Key=dest_key, Body=csv_data)
            csv_keys.append(dest_key)

    return {
        "s3_bucket": bucket,
        "csv_keys": csv_keys,
        "db_config": event["db_config"],
    }
