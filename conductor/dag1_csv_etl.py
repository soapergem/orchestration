"""
DAG 1: CSV ETL Pipeline (Conductor)

Unzip a ZIP of CSVs, load each CSV into Postgres in parallel, run a SQL
transform across the loaded tables, and export the result to Parquet.

Conductor idioms demonstrated:
- Workers are plain functions decorated with `@worker_task`; they POLL the
  server over HTTP and hold no inbound port. The engine never calls them.
- The *orchestration* lives in `workflows/dag1_csv_etl.json`, not here. This
  file contains only the task bodies -- the split that defines Conductor.
- `FORK_JOIN_DYNAMIC`: `prepare_csv_fanout` returns a list of task descriptors
  and the engine materialises one real task per element at runtime. This is
  genuine runtime task creation, not just a parallel-for over fixed cardinality.
- Retry policy is declared on the *task definition* (`taskdefs.json`), separately
  from the workflow that uses the task, so every workflow referencing
  `load_csv_to_postgres` inherits the same backoff.
"""

import csv
import io
import os
import zipfile
from pathlib import Path

import psycopg2
from conductor.client.worker.worker_task import worker_task

# ---- database --------------------------------------------------------------

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "dbname": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}

# Per-(runner, DAG) schema isolation -- see CLAUDE.md. DAG 1 self-creates its
# schema, because every table in it comes from the CSVs themselves.
BAKEOFF_NS = os.environ.get("BAKEOFF_NS", "conductor")
SCHEMA = f"{BAKEOFF_NS}_dag1"


def get_db_connection() -> psycopg2.extensions.connection:
    conn = psycopg2.connect(**DB_CONFIG)
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        cur.execute(f'SET search_path TO "{SCHEMA}"')
    conn.commit()
    return conn


# ---- SQL transform ---------------------------------------------------------

TRANSFORM_SQL = """
CREATE TABLE combined_report AS
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


# ---- tasks -----------------------------------------------------------------


@worker_task(task_definition_name="unzip_file")
def unzip_file(zip_path: str, extract_dir: str = "/tmp/conductor_etl_extracted") -> dict:
    """Extract every CSV from the ZIP onto the worker's local filesystem.

    Note the shared-filesystem assumption: the extracted paths are handed to
    `load_csv_to_postgres` as strings, so that task must run on a worker that
    can see the same disk. Fine here (one host worker pool), and the same
    constraint Prefect's container-per-run mode ran into.
    """
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
                {"file_path": dest_path, "filename": os.path.basename(filename)}
            )

    return {
        "csv_files": csv_files,
        "extract_dir": extract_dir,
        "csv_count": len(csv_files),
    }


@worker_task(task_definition_name="prepare_csv_fanout")
def prepare_csv_fanout(csv_files: list[dict]) -> dict:
    """Build the two structures FORK_JOIN_DYNAMIC needs to fan out at runtime.

    This is the one genuinely awkward part of Conductor's dynamic fork, and it
    is easy to get subtly wrong:

      * `dynamicTasks` -- a list of *task definitions* (dicts shaped like the
        WorkflowTask JSON: name, taskReferenceName, type). One real task is
        created per element.
      * `dynamicTasksInput` -- a map keyed by each task's **taskReferenceName**,
        whose value is that task's input.

    Both must be produced by a worker; there is no expression language that can
    build them from a list inline. The reference names must be unique within the
    workflow, hence the index suffix.
    """
    dynamic_tasks = []
    dynamic_inputs = {}

    for i, csv_file in enumerate(csv_files):
        ref = f"load_csv_{i}"
        table_name = csv_file["filename"].removesuffix(".csv").lower()
        dynamic_tasks.append(
            {
                "name": "load_csv_to_postgres",
                "taskReferenceName": ref,
                "type": "SIMPLE",
            }
        )
        dynamic_inputs[ref] = {
            "file_path": csv_file["file_path"],
            "table_name": table_name,
        }

    return {
        "dynamic_tasks": dynamic_tasks,
        "dynamic_inputs": dynamic_inputs,
        "fanout_width": len(dynamic_tasks),
    }


@worker_task(task_definition_name="load_csv_to_postgres")
def load_csv_to_postgres(file_path: str, table_name: str) -> dict:
    """Load a single CSV into its own Postgres table via COPY.

    Retries and backoff are NOT specified here -- they live on the task
    definition in `taskdefs.json`, which is the Conductor way round.
    """
    rows = list(csv.DictReader(io.StringIO(Path(file_path).read_text(encoding="utf-8"))))
    if not rows:
        return {"table": table_name, "rows_loaded": 0}

    columns = list(rows[0].keys())
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
            cur.execute(f'DROP TABLE IF EXISTS "{table_name}" CASCADE')
            cur.execute(f'CREATE TABLE "{table_name}" ({col_defs})')

            buf = io.StringIO()
            csv.DictWriter(buf, fieldnames=columns).writerows(rows)
            buf.seek(0)
            col_list = ", ".join(f'"{c}"' for c in columns)
            cur.copy_expert(
                f'COPY "{table_name}" ({col_list}) FROM STDIN WITH CSV', buf
            )
        conn.commit()
    finally:
        conn.close()

    return {"table": table_name, "rows_loaded": len(rows)}


@worker_task(task_definition_name="run_sql_transform")
def run_sql_transform() -> dict:
    """JOIN the loaded tables into a single combined_report table."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS combined_report")
            cur.execute(TRANSFORM_SQL)
            cur.execute("SELECT COUNT(*) FROM combined_report")
            row_count = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()

    return {"table": "combined_report", "row_count": row_count}


@worker_task(task_definition_name="convert_to_parquet")
def convert_to_parquet(output_dir: str = "/tmp/conductor_etl_output") -> dict:
    """Read combined_report back out and write it as Parquet."""
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq

    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, "combined_report.parquet")

    conn = get_db_connection()
    try:
        df = pd.read_sql('SELECT * FROM "combined_report"', conn)
    finally:
        conn.close()

    pq.write_table(pa.Table.from_pandas(df), output_path)
    return {"status": "success", "output_path": output_path, "row_count": len(df)}
