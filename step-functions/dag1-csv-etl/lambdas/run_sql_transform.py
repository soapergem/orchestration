"""
Lambda: RunSQLTransform
Connects to Postgres and runs a SQL transformation on the loaded data.
"""

import json
import os

import boto3
import psycopg2


secrets = boto3.client("secretsmanager")

# The SQL transform to run. In practice this might come from S3 or the event input.
TRANSFORM_SQL = """
CREATE TABLE IF NOT EXISTS combined_report AS
SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.email,
    o.product_id,
    p.product_name,
    p.category,
    CAST(o.quantity AS INTEGER) AS quantity,
    CAST(p.price AS NUMERIC(10,2)) AS unit_price,
    CAST(o.quantity AS INTEGER) * CAST(p.price AS NUMERIC(10,2)) AS total_amount,
    o.order_date
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id;
"""


def get_db_connection(db_config):
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
    db_config = event["db_config"]

    conn = get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Drop and recreate the report table
            cur.execute("DROP TABLE IF EXISTS combined_report")
            cur.execute(TRANSFORM_SQL)

            # Get row count for the report
            cur.execute("SELECT COUNT(*) FROM combined_report")
            row_count = cur.fetchone()[0]

        conn.commit()
    finally:
        conn.close()

    return {
        "s3_bucket": event["s3_bucket"],
        "db_config": db_config,
        "transform_result": {
            "table": "combined_report",
            "row_count": row_count,
        },
    }
