"""
DAG 1: CSV ETL Pipeline — Flyte Implementation

Pipeline:
  1. unzip_file       — Extract CSVs from a ZIP archive, return list of paths
  2. load_csv_to_pg   — Load a single CSV into Postgres (mapped via @dynamic)
  3. run_sql_transform — Run a SQL JOIN across the loaded tables
  4. convert_to_parquet — Read the joined table, write a Parquet file (FlyteFile)

Equivalent Step Functions workflow:
  step-functions/dag1-csv-etl/state-machine.asl.json

Key Flyte features demonstrated:
  - @dynamic for fan-out (parallel map over extracted CSV files)
  - RetryStrategy for transient-failure resilience
  - FlyteFile for first-class file output tracking
  - ImageSpec for declaring Python dependencies
  - Strong typing via dataclasses on every task boundary
"""

from __future__ import annotations

import csv
import io
import os
import urllib.request
import zipfile
from typing import List

import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from flytekit import ImageSpec, dynamic, task, workflow
from flytekit.types.file import FlyteFile

from .types import (
    CSVLoadResult,
    DBConfig,
    ETLInput,
    ETLOutput,
    ExtractedCSV,
    TransformResult,
)

# ---------------------------------------------------------------------------
# Container image spec — declares all Python deps needed at runtime.
# Flyte will build/cache this image automatically.
# ---------------------------------------------------------------------------
# A prebuilt image can be substituted for the ImageSpec entirely. ImageSpec
# assumes a working local container builder AND that the build host can produce
# the cluster's architecture -- neither holds when building arm64 from an x86
# workstation without qemu binfmt handlers (rootless podman cannot register
# them). FLYTE_TASK_IMAGE points at an image built natively on the cluster
# instead, and registration then pushes nothing. See flyte/README.md.
_PREBUILT = os.environ.get("FLYTE_TASK_IMAGE")

etl_image = _PREBUILT or ImageSpec(
    name="csv-etl",
    packages=[
        "psycopg2-binary",
        "pyarrow",
        "pandas",
        "flytekit",
    ],
    python_version="3.11",
    # Target platform must match the cluster's node architecture. flytekit
    # defaults this to linux/amd64 (it only picks arm64 when pushing to a LOCAL
    # registry from an arm64 build host), so an arm64 cluster needs it set
    # explicitly -- otherwise the image pulls and schedules fine and then the
    # container dies with "exec format error". See RUNNING.md 7b.
    platform=os.environ.get("FLYTE_IMAGE_PLATFORM", "linux/amd64"),
)

# ---------------------------------------------------------------------------
# SQL transform query (same as the Step Functions version)
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
# Helper: Postgres connection from DBConfig
# ---------------------------------------------------------------------------
def _get_connection(cfg: DBConfig) -> psycopg2.extensions.connection:
    """Connect with ``search_path`` scoped to this runner's DAG 1 schema.

    DAG 1's tables are derived from the CSVs, so the schema is self-creating —
    unlike DAG 3/4, whose schemas hold seeded fixtures and therefore fail fast.
    """
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        dbname=cfg.database,
        user=cfg.user,
        password=cfg.password,
    )
    schema = f"{cfg.namespace}_dag1"
    with conn.cursor() as cur:
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
        cur.execute(f'SET search_path TO "{schema}"')
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Task 1: Unzip file and return CSV paths
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=etl_image,
)
def unzip_file(zip_file_path: str, extract_dir: str) -> List[ExtractedCSV]:
    """Extract all CSV files from a ZIP archive.

    Accepts either a local path or an http(s) URL. The URL form is what makes
    this runnable on Kubernetes: a task pod is ephemeral and shares no
    filesystem with the launcher, so a local path only works for a local run.
    Point it at the fixture-service (`http://fixture-service:8099/sample-data.zip`)
    aliased into the task namespace. Matches how the Argo implementation sources
    the same archive via its `zip-url` parameter.

    Returns a list of absolute paths to the extracted CSVs.
    """
    os.makedirs(extract_dir, exist_ok=True)

    local_zip = zip_file_path
    if zip_file_path.startswith(("http://", "https://")):
        local_zip = os.path.join(extract_dir, "_input.zip")
        with urllib.request.urlopen(zip_file_path, timeout=120) as response:
            with open(local_zip, "wb") as fh:
                fh.write(response.read())

    csv_paths: List[ExtractedCSV] = []
    with zipfile.ZipFile(local_zip, "r") as zf:
        for member in zf.namelist():
            if not member.endswith(".csv"):
                continue
            dest_path = os.path.join(extract_dir, os.path.basename(member))
            with zf.open(member) as src, open(dest_path, "wb") as dst:
                dst.write(src.read())
            table = os.path.basename(member).replace(".csv", "").lower()
            csv_paths.append(ExtractedCSV(table=table, file=FlyteFile(dest_path)))

    if not csv_paths:
        raise ValueError(f"No CSV files found in {zip_file_path}")

    return csv_paths


# ---------------------------------------------------------------------------
# Task 2: Load a single CSV into Postgres
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=etl_image,
)
def load_csv_to_postgres(csv_path: ExtractedCSV, db_config: DBConfig) -> CSVLoadResult:
    """Read a single CSV from *csv_path* and load it into a Postgres table.

    The table name is derived from the filename (e.g. ``users.csv`` -> ``users``).
    All columns are created as TEXT.  The table is truncated and reloaded on
    each invocation so the operation is idempotent.
    """
    table_name = csv_path.table
    # Accessing .path on a FlyteFile input downloads the blob into this pod.
    local_csv = csv_path.file.download()

    with open(local_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        return CSVLoadResult(table=table_name, rows_loaded=0)

    columns = list(rows[0].keys())

    conn = _get_connection(db_config)
    try:
        with conn.cursor() as cur:
            # Create table if not exists (all TEXT for simplicity)
            col_defs = ", ".join(f'"{col}" TEXT' for col in columns)
            cur.execute(f'CREATE TABLE IF NOT EXISTS "{table_name}" ({col_defs})')

            # Truncate and reload
            cur.execute(f'TRUNCATE TABLE "{table_name}"')

            # Bulk insert via COPY
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

    return CSVLoadResult(table=table_name, rows_loaded=len(rows))


# ---------------------------------------------------------------------------
# Dynamic workflow: fan-out CSV loads in parallel
# ---------------------------------------------------------------------------
@dynamic(container_image=etl_image)
def load_all_csvs(csv_paths: List[ExtractedCSV], db_config: DBConfig) -> List[CSVLoadResult]:
    """Dynamically map ``load_csv_to_postgres`` over every extracted CSV.

    Flyte's @dynamic creates one task node per CSV path at runtime, enabling
    parallel execution (equivalent to Step Functions' Map state with
    MaxConcurrency).
    """
    results: List[CSVLoadResult] = []
    for path in csv_paths:
        result = load_csv_to_postgres(csv_path=path, db_config=db_config)
        results.append(result)
    return results


# ---------------------------------------------------------------------------
# Task 3: Run SQL transform (JOIN)
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=etl_image,
)
def run_sql_transform(db_config: DBConfig) -> TransformResult:
    """Drop and recreate the ``combined_report`` table via a SQL JOIN.

    Returns the table name and row count.
    """
    conn = _get_connection(db_config)
    try:
        with conn.cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS combined_report")
            cur.execute(TRANSFORM_SQL)
            cur.execute("SELECT COUNT(*) FROM combined_report")
            row_count: int = cur.fetchone()[0]
        conn.commit()
    finally:
        conn.close()

    return TransformResult(table="combined_report", row_count=row_count)


# ---------------------------------------------------------------------------
# Task 4: Convert to Parquet (output as FlyteFile)
# ---------------------------------------------------------------------------
@task(
    retries=3,
    container_image=etl_image,
)
def convert_to_parquet(
    db_config: DBConfig,
    transform_result: TransformResult,
    output_dir: str,
) -> FlyteFile:
    """Read the transformed table from Postgres and write it as Parquet.

    Returns a ``FlyteFile`` pointing to the Parquet output. Flyte will
    automatically upload this to the configured blob store.
    """
    import pandas as pd

    table_name = transform_result.table

    conn = _get_connection(db_config)
    try:
        df = pd.read_sql(f'SELECT * FROM "{table_name}"', conn)
    finally:
        conn.close()

    os.makedirs(output_dir, exist_ok=True)
    parquet_path = os.path.join(output_dir, f"{table_name}.parquet")

    arrow_table = pa.Table.from_pandas(df)
    pq.write_table(arrow_table, parquet_path)

    return FlyteFile(path=parquet_path)


# ---------------------------------------------------------------------------
# Top-level workflow
# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Output assembler
# ---------------------------------------------------------------------------
# A @workflow body builds a graph; it does not run eagerly, so every task result
# there is a Promise. Constructing a dataclass from Promises fails at
# REGISTRATION with "can not serialize 'Promise' object". Assemble in a @task,
# which receives real values.
@task(container_image=etl_image)
def build_etl_output(
    parquet_file: FlyteFile,
    row_count: int,
    tables_loaded: List[CSVLoadResult],
) -> ETLOutput:
    """Assemble the workflow output.

    Takes the FlyteFile itself, not `parquet_file.path`: inside a @workflow that
    attribute is not a string but part of a blob-typed Promise, and registration
    rejects the binding with "output variable 'convert_to_parquet.o0' has type
    [blob:{}], but it's assigned to ... type [simple:STRING]". Here the value is
    real, and `remote_source` is the durable blob URI -- more useful to record
    than the ephemeral in-pod path.
    """
    return ETLOutput(
        status="success",
        parquet_path=parquet_file.remote_source or parquet_file.path,
        row_count=row_count,
        tables_loaded=tables_loaded,
    )


@workflow
def csv_etl_pipeline(etl_input: ETLInput) -> ETLOutput:
    """CSV ETL Pipeline.

    1. Unzip the archive to extract CSVs.
    2. Fan-out: load each CSV into Postgres in parallel.
    3. Run a SQL JOIN across the loaded tables.
    4. Export the joined result as a Parquet file.
    """
    csv_paths = unzip_file(
        zip_file_path=etl_input.zip_file_path,
        extract_dir=etl_input.extract_dir,
    )

    load_results = load_all_csvs(
        csv_paths=csv_paths,
        db_config=etl_input.db_config,
    )

    transform_result = run_sql_transform(db_config=etl_input.db_config)
    # `>>` is REQUIRED here, not stylistic. A @workflow body's statement order
    # implies nothing: Flyte derives the graph purely from data dependencies, and
    # this task consumes none of the preceding task's outputs, so without an
    # explicit edge Flyte schedules them in PARALLEL. Verified on the cluster --
    # run_sql_transform started alongside unzip_file and died with
    # 'relation "orders" does not exist'.
    load_results >> transform_result

    parquet_file = convert_to_parquet(
        db_config=etl_input.db_config,
        transform_result=transform_result,
        output_dir=etl_input.output_dir,
    )

    return build_etl_output(
        parquet_file=parquet_file,
        row_count=transform_result.row_count,
        tables_loaded=load_results,
    )
