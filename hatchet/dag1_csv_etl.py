"""
DAG 1: CSV ETL Pipeline

Unzips a file containing CSVs, loads each CSV into Postgres in parallel
(via child workflow spawning), runs a SQL transform, and exports to Parquet.

Hatchet features used:
- hatchet.workflow / @wf.task for workflow definition
- child workflow spawning (aio_run_no_wait) for fan-out child processing
- Task-level retries with backoff
- DAG-style sequential task dependencies
"""

import csv
import io
import os
import zipfile
from pathlib import Path

import psycopg2
from hatchet_sdk import Context, Hatchet

hatchet = Hatchet()

# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}

# Per-(runner, DAG) schema isolation -- see CLAUDE.md. DAG 1 self-creates its
# schema because every table here comes from the CSVs themselves.
BAKEOFF_NS = os.environ.get("BAKEOFF_NS", "hatchet")
SCHEMA = f"{BAKEOFF_NS}_dag1"


def get_db_connection(db_config: dict | None = None) -> psycopg2.extensions.connection:
    cfg = db_config or DB_CONFIG
    conn = psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        dbname=cfg.get("dbname", cfg.get("database", "orchestration")),
        user=cfg["user"],
        password=cfg["password"],
    )
    schema = cfg.get("schema", SCHEMA)
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute(f'SET search_path TO "{schema}"')
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# SQL transform (same as Step Functions version)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Child workflow: load a single CSV into Postgres
# ---------------------------------------------------------------------------

load_csv_wf = hatchet.workflow(name="LoadCSVToPostgres", on_events=["csv:load"])


@load_csv_wf.task(name="load_csv", retries=3, backoff_factor=2.0, backoff_max_seconds=5)
async def load_csv(input: dict, context: Context) -> dict:
    """Loads a single CSV file into a Postgres table."""
    input = input.model_dump()
    file_path = input["file_path"]
    table_name = input["table_name"]
    db_config = input.get("db_config") or DB_CONFIG

    # Read CSV content
    csv_text = Path(file_path).read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = list(reader)

    if not rows:
        return {"table": table_name, "rows_loaded": 0}

    columns = list(rows[0].keys())

    conn = get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Create table if not exists (all columns as TEXT for simplicity)
            col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
            cur.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

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


# ---------------------------------------------------------------------------
# Main ETL workflow
# ---------------------------------------------------------------------------

csv_etl_wf = hatchet.workflow(name="CSVETLPipeline", on_events=["etl:csv_pipeline"])


@csv_etl_wf.task(name="unzip_file", retries=3, backoff_factor=2.0, backoff_max_seconds=2)
async def unzip_file(input: dict, context: Context) -> dict:
    """Downloads/reads a ZIP file and extracts CSV files to a local directory."""
    input = input.model_dump()
    zip_path = input["zip_path"]
    extract_dir = input.get("extract_dir", "/tmp/etl_extracted")

    os.makedirs(extract_dir, exist_ok=True)
    csv_files = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        for filename in zf.namelist():
            if not filename.endswith(".csv"):
                continue

            dest_path = os.path.join(extract_dir, os.path.basename(filename))
            with zf.open(filename) as src, open(dest_path, "wb") as dst:
                dst.write(src.read())
            csv_files.append(
                {
                    "file_path": dest_path,
                    "filename": os.path.basename(filename),
                }
            )

    return {
        "csv_files": csv_files,
        "extract_dir": extract_dir,
        "csv_count": len(csv_files),
    }


@csv_etl_wf.task(
    name="process_csvs",
    parents=[unzip_file],
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=2,
)
async def process_csvs(input: dict, context: Context) -> dict:
    """Fan-out: spawn a child workflow for each CSV file to load into Postgres."""
    input = input.model_dump()
    unzip_result = context.task_output(unzip_file)
    csv_files = unzip_result["csv_files"]
    db_config = input.get("db_config") or DB_CONFIG

    # Spawn child workflows for each CSV -- bulk fan-out
    child_results = []
    spawn_refs = []

    for csv_file in csv_files:
        table_name = csv_file["filename"].replace(".csv", "").lower()
        child_input = {
            "file_path": csv_file["file_path"],
            "table_name": table_name,
            "db_config": db_config,
        }
        ref = await load_csv_wf.aio_run_no_wait(
            child_input,
            child_key=f"load-csv-{table_name}",
        )
        spawn_refs.append(ref)

    # Wait for all child workflows to complete. aio_result() returns the child
    # workflow's output keyed by task name, so unwrap the single load_csv task.
    for ref in spawn_refs:
        result = await ref.aio_result()
        child_results.append(result["load_csv"])

    return {
        "load_results": child_results,
        "total_csvs_processed": len(child_results),
    }


@csv_etl_wf.task(
    name="run_sql_transform",
    parents=[process_csvs],
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=10,
)
async def run_sql_transform(input: dict, context: Context) -> dict:
    """Run a SQL transform joining the loaded tables into a combined report."""
    input = input.model_dump()
    db_config = input.get("db_config") or DB_CONFIG

    conn = get_db_connection(db_config)
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
        "transform_result": {
            "table": "combined_report",
            "row_count": row_count,
        },
    }


@csv_etl_wf.task(
    name="convert_to_parquet",
    parents=[run_sql_transform],
    retries=3,
    backoff_factor=2.0,
    backoff_max_seconds=2,
)
async def convert_to_parquet(input: dict, context: Context) -> dict:
    """Read the transformed data from Postgres and write it as a Parquet file."""
    input = input.model_dump()
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    db_config = input.get("db_config") or DB_CONFIG
    output_dir = input.get("output_dir", "/tmp/etl_output")
    table_name = "combined_report"

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"{table_name}.parquet")

    conn = get_db_connection(db_config)
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
