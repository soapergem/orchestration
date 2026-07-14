"""
Lambda: ConvertToParquet
Reads the transformed data from Postgres and writes it as a Parquet file to S3.

Builds the Arrow table directly from the DB cursor (no pandas) so the layer
stays well under the Lambda size limit; the Parquet output is unchanged.
"""

import io

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from db import get_db_connection


s3 = boto3.client("s3")


def handler(event, context):
    bucket = event["s3_bucket"]
    table_name = event["transform_result"]["table"]
    output_key = f"output/{table_name}.parquet"

    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f'SELECT * FROM "{table_name}"')
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()

    # Columnar dict -> Arrow table. pyarrow infers types (Decimal -> decimal128,
    # int -> int64, str -> string) and handles NULLs.
    data = {col: [row[i] for row in rows] for i, col in enumerate(columns)}
    table = pa.table(data)

    buf = io.BytesIO()
    pq.write_table(table, buf)
    s3.put_object(Bucket=bucket, Key=output_key, Body=buf.getvalue())

    return {
        "status": "success",
        "output_bucket": bucket,
        "output_key": output_key,
        "row_count": len(rows),
    }
