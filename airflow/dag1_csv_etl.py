"""
DAG 1: CSV ETL Pipeline (Classic Operator Style with >> Operator)

Unzip a file containing CSVs, load each CSV into Postgres in parallel using
dynamic task mapping, run a SQL JOIN transform across the loaded tables, and
export the result as a Parquet file.

Airflow idioms used:
- Classic operator instantiation (PythonOperator)
- Explicit dag= parameter on each operator (no context manager)
- >> operator for dependency chaining
- Dynamic task mapping (.expand()) on a mapped PythonOperator
- psycopg2 for DB operations
- pandas + pyarrow for Parquet conversion
"""

from __future__ import annotations

import csv
import io
import json
import os
import zipfile
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from airflow import DAG
from airflow.operators.python import PythonOperator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DB_CONN_PARAMS = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}

DEFAULT_ZIP_PATH = os.environ.get("ETL_ZIP_PATH", "/opt/airflow/data/input.zip")
DEFAULT_EXTRACT_DIR = os.environ.get("ETL_EXTRACT_DIR", "/opt/airflow/data/extracted")
DEFAULT_OUTPUT_DIR = os.environ.get("ETL_OUTPUT_DIR", "/opt/airflow/data/output")

TRANSFORM_SQL = """\
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


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_connection() -> psycopg2.extensions.connection:
    return psycopg2.connect(**DB_CONN_PARAMS)


# ---------------------------------------------------------------------------
# Callable functions for PythonOperators
# ---------------------------------------------------------------------------

def unzip_file_fn(
    zip_path: str = DEFAULT_ZIP_PATH,
    extract_dir: str = DEFAULT_EXTRACT_DIR,
    **context,
) -> list[str]:
    """Extract CSVs from a ZIP archive and return their file paths."""
    os.makedirs(extract_dir, exist_ok=True)

    csv_paths: list[str] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if not name.endswith(".csv"):
                continue
            dest = os.path.join(extract_dir, os.path.basename(name))
            with zf.open(name) as src, open(dest, "wb") as dst:
                dst.write(src.read())
            csv_paths.append(dest)

    if not csv_paths:
        raise ValueError(f"No CSV files found in {zip_path}")

    return csv_paths


def load_csv_to_postgres_fn(csv_path: str, **context) -> dict:
    """Load one CSV file into a Postgres table named after the file."""
    filename = os.path.basename(csv_path)
    table_name = filename.replace(".csv", "").lower()

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return {"table": table_name, "rows_loaded": 0}

    columns = list(rows[0].keys())

    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')
            cur.execute(f'TRUNCATE TABLE "{table_name}"')

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


def run_sql_transform_fn(load_results: list[dict] | None = None, **context) -> dict:
    """Run SQL JOIN across loaded tables to create combined_report."""
    ti = context["task_instance"]
    if load_results is None:
        load_results = ti.xcom_pull(task_ids="load_csv_to_postgres")

    tables_loaded = [r["table"] for r in load_results] if load_results else []

    conn = _get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS combined_report")
            cur.execute(TRANSFORM_SQL)
            cur.execute("SELECT COUNT(*) FROM combined_report")
            row_count = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()

    return {
        "table": "combined_report",
        "row_count": row_count,
        "source_tables": tables_loaded,
    }


def convert_to_parquet_fn(output_dir: str = DEFAULT_OUTPUT_DIR, **context) -> dict:
    """Read combined_report from Postgres and write as a Parquet file."""
    ti = context["task_instance"]
    transform_result = ti.xcom_pull(task_ids="run_sql_transform")
    table_name = transform_result["table"]

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{table_name}.parquet")

    conn = _get_connection()
    try:
        df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    finally:
        conn.close()

    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_path)

    return {
        "status": "success",
        "output_path": output_path,
        "row_count": len(df),
    }


# ---------------------------------------------------------------------------
# DAG definition using explicit dag= parameter (no context manager)
# ---------------------------------------------------------------------------

dag = DAG(
    dag_id="dag1_csv_etl",
    description="CSV ETL Pipeline: unzip, parallel CSV load, SQL transform, Parquet export",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args={
        "owner": "orchestration",
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=2),
    },
    tags=["etl", "csv", "postgres", "parquet"],
)

unzip_file = PythonOperator(
    task_id="unzip_file",
    python_callable=unzip_file_fn,
    dag=dag,
)

# Dynamic task mapping: one load_csv_to_postgres per CSV.
# .expand() works on PythonOperator by mapping over op_args or op_kwargs.
load_csv_to_postgres = PythonOperator.partial(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_to_postgres_fn,
    dag=dag,
).expand(
    op_kwargs=unzip_file.output.map(lambda path: {"csv_path": path}),
)

run_sql_transform = PythonOperator(
    task_id="run_sql_transform",
    python_callable=run_sql_transform_fn,
    dag=dag,
)

convert_to_parquet = PythonOperator(
    task_id="convert_to_parquet",
    python_callable=convert_to_parquet_fn,
    dag=dag,
)

# Wire dependencies using the >> operator
unzip_file >> load_csv_to_postgres >> run_sql_transform >> convert_to_parquet
