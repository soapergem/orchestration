"""
Lambda: LoadCSVToPostgres
Reads a single CSV from S3 and loads it into a Postgres table.
"""

import csv
import io
import json
import os

import boto3
import psycopg2


s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")


def get_db_connection(db_config):
    """Get a Postgres connection using credentials from Secrets Manager."""
    secret = secrets.get_secret_value(SecretId=db_config["secret_arn"])
    creds = json.loads(secret["SecretString"])

    return psycopg2.connect(
        host=db_config["host"],
        database=db_config["database"],
        user=creds["username"],
        password=creds["password"],
        port=creds.get("port", 5432),
    )


def handler(event, context):
    bucket = event["s3_bucket"]
    s3_key = event["s3_key"]
    db_config = event["db_config"]

    # Derive table name from filename (e.g., "extracted/users.csv" -> "users")
    filename = s3_key.rsplit("/", 1)[-1]
    table_name = filename.replace(".csv", "").lower()

    # Download CSV from S3
    csv_obj = s3.get_object(Bucket=bucket, Key=s3_key)
    csv_text = csv_obj["Body"].read().decode("utf-8")
    reader = csv.DictReader(io.StringIO(csv_text))

    rows = list(reader)
    if not rows:
        return {"table": table_name, "rows_loaded": 0}

    columns = list(rows[0].keys())

    conn = get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Create table if not exists (simple approach: all columns as TEXT)
            col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

            # Truncate and reload
            cur.execute(f'TRUNCATE TABLE "{table_name}"')

            # Bulk insert using COPY
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=columns)
            writer.writerows(rows)
            buf.seek(0)

            cur.copy_expert(
                f"""COPY "{table_name}" ({", ".join(f'"{c}"' for c in columns)})
                    FROM STDIN WITH CSV""",
                buf,
            )

        conn.commit()
    finally:
        conn.close()

    return {"table": table_name, "rows_loaded": len(rows)}
