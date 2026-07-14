"""
Lambda: RunSQLTransform
Connects to Postgres and runs a SQL transformation on the loaded data.
"""

from db import get_db_connection

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


def handler(event, context):
    conn = get_db_connection()
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
        "transform_result": {
            "table": "combined_report",
            "row_count": row_count,
        },
    }
