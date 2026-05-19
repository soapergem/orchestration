"""
DAG 1 -- CSV ETL Pipeline (Dagster)

Workflow:
  1. unzip_file        -- Download a ZIP from a local/remote path, extract CSVs
  2. load_csv_to_postgres -- DynamicOutput fan-out: load each CSV into its own table
  3. run_sql_transform  -- SQL JOIN across the loaded tables
  4. convert_to_parquet -- Read the joined table back out as a Parquet file

All ops use RetryPolicy(max_retries=3, delay=5).  The parallel fan-out uses
Dagster's DynamicOutput / map() mechanism.  A ``postgres_resource`` is injected
via the Dagster resource system rather than connecting to Secrets Manager.
"""

import csv
import io
import os
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dagster import (
    DynamicOut,
    DynamicOutput,
    In,
    Nothing,
    Out,
    Output,
    RetryPolicy,
    graph,
    job,
    op,
)

from .resources import PostgresResource

# ---------------------------------------------------------------------------
# Constants
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

RETRY = RetryPolicy(max_retries=3, delay=5)

# ---------------------------------------------------------------------------
# Ops
# ---------------------------------------------------------------------------


@op(
    description="Download a ZIP file and extract all CSV files to a local directory.",
    out=DynamicOut(str),
    retry_policy=RETRY,
    config_schema={"zip_path": str, "extract_dir": str},
)
def unzip_file(context) -> DynamicOutput:
    """Extract CSVs from a ZIP archive.

    ``zip_path`` can be a local filesystem path.  In a production variant it
    could download from S3/GCS first.  Each extracted CSV path is yielded as a
    separate DynamicOutput so downstream ops can fan out.
    """
    zip_path = context.op_config["zip_path"]
    extract_dir = context.op_config["extract_dir"]
    os.makedirs(extract_dir, exist_ok=True)

    context.log.info(f"Extracting ZIP {zip_path} to {extract_dir}")

    csv_paths: list[str] = []
    with zipfile.ZipFile(zip_path, "r") as zf:
        for filename in zf.namelist():
            if not filename.endswith(".csv"):
                continue
            dest = os.path.join(extract_dir, os.path.basename(filename))
            with open(dest, "wb") as fout:
                fout.write(zf.read(filename))
            csv_paths.append(dest)

    context.log.info(f"Extracted {len(csv_paths)} CSV file(s)")

    for idx, csv_path in enumerate(csv_paths):
        mapping_key = Path(csv_path).stem.replace("-", "_").replace(" ", "_")
        yield DynamicOutput(csv_path, mapping_key=mapping_key)


@op(
    description="Load a single CSV file into a Postgres table (truncate-and-reload).",
    retry_policy=RETRY,
    out=Out(dict),
)
def load_csv_to_postgres(context, csv_path: str, postgres: PostgresResource) -> dict:
    """Create the target table (all TEXT columns), TRUNCATE, and COPY the CSV data in."""
    filename = os.path.basename(csv_path)
    table_name = filename.replace(".csv", "").lower()

    with open(csv_path, "r", newline="") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        context.log.warning(f"CSV {filename} is empty -- skipping load")
        return {"table": table_name, "rows_loaded": 0}

    columns = list(rows[0].keys())

    with postgres.get_connection() as conn:
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

    context.log.info(f"Loaded {len(rows)} rows into {table_name}")
    return {"table": table_name, "rows_loaded": len(rows)}


@op(
    description="Run a SQL transformation that JOINs the loaded CSV tables.",
    retry_policy=RETRY,
    ins={"load_results": In(list)},
    out=Out(dict),
)
def run_sql_transform(context, load_results: list, postgres: PostgresResource) -> dict:
    """Drop and recreate ``combined_report`` via a multi-table JOIN."""
    context.log.info(f"Running SQL transform after loading {len(load_results)} tables")

    with postgres.get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS combined_report")
            cur.execute(TRANSFORM_SQL)
            cur.execute("SELECT COUNT(*) FROM combined_report")
            row_count = cur.fetchone()[0]
        conn.commit()

    context.log.info(f"combined_report created with {row_count} rows")
    return {"table": "combined_report", "row_count": row_count}


@op(
    description="Read transformed data from Postgres and write as Parquet.",
    retry_policy=RETRY,
    out=Out(dict),
    config_schema={"output_path": str},
)
def convert_to_parquet(context, transform_result: dict, postgres: PostgresResource) -> dict:
    """Export ``combined_report`` to a local Parquet file."""
    table_name = transform_result["table"]
    output_path = context.op_config["output_path"]
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    with postgres.get_connection() as conn:
        df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)

    arrow_table = pa.Table.from_pandas(df)
    pq.write_table(arrow_table, output_path)

    context.log.info(f"Wrote {len(df)} rows to {output_path}")
    return {
        "status": "success",
        "output_path": output_path,
        "row_count": len(df),
    }


# ---------------------------------------------------------------------------
# Graph / Job
# ---------------------------------------------------------------------------


@graph
def csv_etl_graph():
    csv_paths = unzip_file()
    load_results = csv_paths.map(
        lambda csv_path: load_csv_to_postgres(csv_path)
    ).collect()
    transform_result = run_sql_transform(load_results)
    convert_to_parquet(transform_result)


csv_etl_job = csv_etl_graph.to_job(
    name="csv_etl_job",
    description="CSV ETL Pipeline: unzip, parallel load to Postgres, SQL transform, Parquet export.",
    resource_defs={
        "postgres": PostgresResource(
            host="postgres",
            port=5432,
            database="orchestration",
            user="orchestration",
            password="orchestration",
        ),
    },
)
