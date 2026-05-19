"""
DAG 1: CSV ETL Pipeline
========================
Unzip a file containing CSVs, load each CSV into Postgres in parallel,
run a SQL JOIN transform, and export the result to Parquet.

Prefect 3.x implementation using @flow, @task, and .map() for fan-out.
"""

import csv
import io
import os
import zipfile
from pathlib import Path

import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from prefect import flow, get_run_logger, task

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

DB_CONFIG = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
    "database": os.environ.get("POSTGRES_DB", "orchestration"),
    "user": os.environ.get("POSTGRES_USER", "orchestration"),
    "password": os.environ.get("POSTGRES_PASSWORD", "orchestration"),
}

# Default local directories (override via flow parameters)
DEFAULT_ZIP_PATH = "/data/input/data.zip"
DEFAULT_EXTRACT_DIR = "/data/extracted"
DEFAULT_OUTPUT_DIR = "/data/output"

# ---------------------------------------------------------------------------
# SQL transform — mirrors the Step Functions implementation
# ---------------------------------------------------------------------------

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
# Helper
# ---------------------------------------------------------------------------

def _get_db_connection(db_config: dict | None = None):
    """Return a psycopg2 connection using the provided or default config."""
    cfg = db_config or DB_CONFIG
    return psycopg2.connect(
        host=cfg["host"],
        port=cfg.get("port", 5432),
        dbname=cfg["database"],
        user=cfg["user"],
        password=cfg["password"],
    )


# ---------------------------------------------------------------------------
# Tasks
# ---------------------------------------------------------------------------

@task(
    retries=3,
    retry_delay_seconds=[5, 10, 20],
    name="unzip_file",
)
def unzip_file(zip_path: str, extract_dir: str) -> list[str]:
    """Extract all CSVs from *zip_path* into *extract_dir*. Return paths."""
    logger = get_run_logger()
    extract_path = Path(extract_dir)
    extract_path.mkdir(parents=True, exist_ok=True)

    csv_paths: list[str] = []

    with zipfile.ZipFile(zip_path, "r") as zf:
        for name in zf.namelist():
            if not name.endswith(".csv"):
                continue
            dest = extract_path / Path(name).name
            with zf.open(name) as src, open(dest, "wb") as dst:
                dst.write(src.read())
            csv_paths.append(str(dest))
            logger.info("Extracted %s -> %s", name, dest)

    logger.info("Extracted %d CSV files", len(csv_paths))
    return csv_paths


@task(
    retries=3,
    retry_delay_seconds=[5, 10, 20],
    name="load_csv_to_postgres",
)
def load_csv_to_postgres(csv_path: str, db_config: dict | None = None) -> dict:
    """Load a single CSV file into Postgres (table name derived from filename)."""
    logger = get_run_logger()

    filename = Path(csv_path).stem.lower()
    table_name = filename

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        logger.warning("CSV %s is empty — skipping", csv_path)
        return {"table": table_name, "rows_loaded": 0}

    columns = list(rows[0].keys())

    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Create table (all TEXT columns — matches the Step Functions approach)
            col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

            # Truncate and reload
            cur.execute(f'TRUNCATE TABLE "{table_name}"')

            # Bulk load via COPY
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
        logger.info("Loaded %d rows into table '%s'", len(rows), table_name)
    finally:
        conn.close()

    return {"table": table_name, "rows_loaded": len(rows)}


@task(
    retries=3,
    retry_delay_seconds=[5, 10, 20],
    name="run_sql_transform",
)
def run_sql_transform(db_config: dict | None = None) -> dict:
    """Run the JOIN transform, creating the combined_report table."""
    logger = get_run_logger()

    conn = _get_db_connection(db_config)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS combined_report")
            cur.execute(TRANSFORM_SQL)
            cur.execute("SELECT COUNT(*) FROM combined_report")
            row_count = cur.fetchone()[0]
        conn.commit()
        logger.info("SQL transform complete — combined_report has %d rows", row_count)
    finally:
        conn.close()

    return {"table": "combined_report", "row_count": row_count}


@task(
    retries=3,
    retry_delay_seconds=[5, 10, 20],
    name="convert_to_parquet",
)
def convert_to_parquet(
    table_name: str,
    output_dir: str,
    db_config: dict | None = None,
) -> dict:
    """Read *table_name* from Postgres and write a local Parquet file."""
    logger = get_run_logger()

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    parquet_file = output_path / f"{table_name}.parquet"

    conn = _get_db_connection(db_config)
    try:
        df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    finally:
        conn.close()

    arrow_table = pa.Table.from_pandas(df)
    pq.write_table(arrow_table, str(parquet_file))

    logger.info(
        "Wrote %d rows to %s",
        len(df),
        parquet_file,
    )

    return {
        "status": "success",
        "output_path": str(parquet_file),
        "row_count": len(df),
    }


# ---------------------------------------------------------------------------
# Flow
# ---------------------------------------------------------------------------

@flow(name="csv_etl_pipeline", log_prints=True)
def csv_etl_pipeline(
    zip_path: str = DEFAULT_ZIP_PATH,
    extract_dir: str = DEFAULT_EXTRACT_DIR,
    output_dir: str = DEFAULT_OUTPUT_DIR,
    db_config: dict | None = None,
) -> dict:
    """
    End-to-end CSV ETL pipeline:
      1. Unzip
      2. Load each CSV to Postgres (parallel via .map())
      3. SQL JOIN transform
      4. Export to Parquet
    """
    logger = get_run_logger()
    cfg = db_config or DB_CONFIG

    # Step 1: Extract CSVs
    csv_paths = unzip_file(zip_path, extract_dir)

    # Step 2: Fan-out — load each CSV in parallel
    load_results = load_csv_to_postgres.map(csv_paths, db_config=cfg)

    # Wait for all loads to complete before transforming
    load_summaries = [r.result() for r in load_results]
    logger.info(
        "Loaded %d tables: %s",
        len(load_summaries),
        [s["table"] for s in load_summaries],
    )

    # Step 3: SQL transform (join all loaded tables)
    transform_result = run_sql_transform(db_config=cfg)

    # Step 4: Export to Parquet
    parquet_result = convert_to_parquet(
        table_name=transform_result["table"],
        output_dir=output_dir,
        db_config=cfg,
    )

    return {
        "status": "success",
        "tables_loaded": load_summaries,
        "transform": transform_result,
        "parquet": parquet_result,
    }


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    csv_etl_pipeline()
