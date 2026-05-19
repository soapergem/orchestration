"""
DAG 1: CSV ETL Pipeline

Unzips a file containing CSVs, loads each CSV into Postgres in parallel
(via child workflow spawning), runs a SQL transform, and exports to Parquet.

Hatchet features used:
- @hatchet.workflow / @hatchet.task for workflow definition
- context.spawn_workflow() for fan-out child processing
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


def get_db_connection(db_config: dict | None = None) -> psycopg2.extensions.connection:
    cfg = db_config or DB_CONFIG
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        dbname=cfg.get("dbname", cfg.get("database", "orchestration")),
        user=cfg["user"],
        password=cfg["password"],
    )


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

@hatchet.workflow(name="LoadCSVToPostgres", on_events=["csv:load"])
class LoadCSVToPostgresWorkflow:
    """Loads a single CSV file into a Postgres table."""

    @hatchet.task(name="load_csv", retries=3, backoff_factor=2.0, backoff_base_seconds=5)
    async def load_csv(self, context: Context) -> dict:
        input_data = context.workflow_input()
        file_path = input_data["file_path"]
        table_name = input_data["table_name"]
        db_config = input_data.get("db_config") or DB_CONFIG

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


# ---------------------------------------------------------------------------
# Main ETL workflow
# ---------------------------------------------------------------------------

@hatchet.workflow(name="CSVETLPipeline", on_events=["etl:csv_pipeline"])
class CSVETLPipelineWorkflow:
    """
    CSV ETL Pipeline:
    1. Unzip file -> extract CSVs
    2. Fan-out: spawn child workflows to load each CSV in parallel
    3. Run SQL transform on loaded data
    4. Export transformed data to Parquet
    """

    @hatchet.task(name="unzip_file", retries=3, backoff_factor=2.0, backoff_base_seconds=2)
    async def unzip_file(self, context: Context) -> dict:
        """Downloads/reads a ZIP file and extracts CSV files to a local directory."""
        input_data = context.workflow_input()
        zip_path = input_data["zip_path"]
        extract_dir = input_data.get("extract_dir", "/tmp/etl_extracted")

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

    @hatchet.task(
        name="process_csvs",
        parents=["unzip_file"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def process_csvs(self, context: Context) -> dict:
        """Fan-out: spawn a child workflow for each CSV file to load into Postgres."""
        unzip_result = (await context.task_output("unzip_file"))
        csv_files = unzip_result["csv_files"]
        input_data = context.workflow_input()
        db_config = input_data.get("db_config") or DB_CONFIG

        # Spawn child workflows for each CSV -- bulk fan-out
        child_results = []
        spawn_futures = []

        for csv_file in csv_files:
            table_name = csv_file["filename"].replace(".csv", "").lower()
            child_input = {
                "file_path": csv_file["file_path"],
                "table_name": table_name,
                "db_config": db_config,
            }
            future = context.spawn_workflow(
                "LoadCSVToPostgres",
                child_input,
                key=f"load-csv-{table_name}",
            )
            spawn_futures.append(future)

        # Wait for all child workflows to complete
        for future in spawn_futures:
            result = await future.result()
            child_results.append(result)

        return {
            "load_results": child_results,
            "total_csvs_processed": len(child_results),
        }

    @hatchet.task(
        name="run_sql_transform",
        parents=["process_csvs"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=10,
    )
    async def run_sql_transform(self, context: Context) -> dict:
        """Run a SQL transform joining the loaded tables into a combined report."""
        input_data = context.workflow_input()
        db_config = input_data.get("db_config") or DB_CONFIG

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

    @hatchet.task(
        name="convert_to_parquet",
        parents=["run_sql_transform"],
        retries=3,
        backoff_factor=2.0,
        backoff_base_seconds=2,
    )
    async def convert_to_parquet(self, context: Context) -> dict:
        """Read the transformed data from Postgres and write it as a Parquet file."""
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq

        input_data = context.workflow_input()
        db_config = input_data.get("db_config") or DB_CONFIG
        output_dir = input_data.get("output_dir", "/tmp/etl_output")
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
