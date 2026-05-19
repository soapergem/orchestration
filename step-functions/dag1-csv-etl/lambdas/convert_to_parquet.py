"""
Lambda: ConvertToParquet
Reads the transformed data from Postgres and writes it as a Parquet file to S3.
"""

import io
import json

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import psycopg2


s3 = boto3.client("s3")
secrets = boto3.client("secretsmanager")


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
    bucket = event["s3_bucket"]
    table_name = event["transform_result"]["table"]
    output_key = f"output/{table_name}.parquet"

    conn = get_db_connection(db_config)
    try:
        df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    finally:
        conn.close()

    # Write Parquet to a buffer and upload to S3
    buf = io.BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buf)
    buf.seek(0)

    s3.put_object(Bucket=bucket, Key=output_key, Body=buf.getvalue())

    return {
        "status": "success",
        "output_bucket": bucket,
        "output_key": output_key,
        "row_count": len(df),
    }
