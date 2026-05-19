"""
DAG 1: CSV ETL Pipeline — Luigi Implementation

Pipeline: UnzipFile -> ProcessAllCSVs (one LoadCSVToPostgres per CSV) -> RunSQLTransform -> ConvertToParquet

Mirrors the Step Functions implementation in step-functions/dag1-csv-etl/.

DIVERGENCES FROM STEP FUNCTIONS:
- Luigi uses file-based targets for idempotency (if output file exists, task is skipped).
  Step Functions uses execution-level deduplication and Lambda idempotency keys.
- Luigi parallelism is controlled by --workers N at the CLI level. The Map state in
  Step Functions supports explicit MaxConcurrency within the workflow definition.
- Luigi has no built-in retry with backoff. Step Functions provides declarative Retry
  with IntervalSeconds, MaxAttempts, BackoffRate, and JitterStrategy.
- Luigi has no Catch/error-routing mechanism. Step Functions can route errors to specific
  states. In Luigi, if a task fails, the scheduler marks it failed and stops dependents.
- Luigi has no equivalent of Step Functions' Fail state with structured error metadata.
  Errors surface as Python exceptions in logs.

Run with:
    luigi --module dag1_csv_etl ConvertToParquet \
        --zip-path /path/to/data.zip \
        --extract-dir /tmp/extracted \
        --run-id my-run-001 \
        --workers 4

    The --workers flag controls how many LoadCSVToPostgres tasks run in parallel.
"""

import csv
import io
import json
import os
import zipfile
from pathlib import Path

import luigi
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq


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

MARKER_DIR = os.environ.get("LUIGI_MARKER_DIR", "/tmp/luigi-markers/dag1")

# The SQL transform to run. Matches step-functions/dag1-csv-etl/lambdas/run_sql_transform.py.
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


def get_db_connection():
    """Get a Postgres connection using the shared DB_CONFIG."""
    return psycopg2.connect(
        host=DB_CONFIG["host"],
        port=DB_CONFIG["port"],
        database=DB_CONFIG["database"],
        user=DB_CONFIG["user"],
        password=DB_CONFIG["password"],
    )


# ---------------------------------------------------------------------------
# Task 1: UnzipFile
# ---------------------------------------------------------------------------


class UnzipFile(luigi.Task):
    """
    Extracts CSV files from a ZIP archive to a local directory.

    In Step Functions, this downloads from S3 and writes back to S3.
    In Luigi, we work with local filesystem paths since Luigi's native target
    model is file-based. S3 support exists via luigi.contrib.s3 but this
    implementation uses local paths for simplicity and testability.
    """

    zip_path = luigi.Parameter(description="Path to the input ZIP file")
    extract_dir = luigi.Parameter(description="Directory to extract CSVs into")
    run_id = luigi.Parameter(description="Unique run identifier for idempotency markers")

    def output(self):
        """
        Returns a LocalTarget for the extraction marker file.
        The marker file contains the list of extracted CSV paths as JSON.
        """
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "unzip_complete.json")
        )

    def run(self):
        extract_path = Path(self.extract_dir)
        extract_path.mkdir(parents=True, exist_ok=True)

        csv_paths = []

        with zipfile.ZipFile(self.zip_path, "r") as zf:
            for filename in zf.namelist():
                if not filename.endswith(".csv"):
                    continue

                dest = extract_path / filename
                dest.parent.mkdir(parents=True, exist_ok=True)

                with zf.open(filename) as src, open(dest, "wb") as dst:
                    dst.write(src.read())

                csv_paths.append(str(dest))

        # Write the marker file with the list of CSV paths
        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as f:
            json.dump({"csv_paths": csv_paths, "extract_dir": str(extract_path)}, f)


# ---------------------------------------------------------------------------
# Task 2: LoadCSVToPostgres (one per CSV file)
# ---------------------------------------------------------------------------


class LoadCSVToPostgres(luigi.Task):
    """
    Loads a single CSV file into a Postgres table.

    DIVERGENCE: Step Functions uses a Map state with MaxConcurrency=10 to run
    these in parallel with automatic retry (IntervalSeconds=5, MaxAttempts=2,
    BackoffRate=2.0). Luigi achieves parallelism via --workers N at the CLI,
    but has no built-in retry or backoff. If this task fails, Luigi marks it
    failed and dependents do not run.
    """

    csv_path = luigi.Parameter(description="Path to the CSV file to load")
    run_id = luigi.Parameter(description="Unique run identifier for idempotency markers")

    def output(self):
        """Marker file indicating this CSV was loaded successfully."""
        csv_name = os.path.basename(self.csv_path).replace(".csv", "")
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, f"loaded_{csv_name}.json")
        )

    def run(self):
        filename = os.path.basename(self.csv_path)
        table_name = filename.replace(".csv", "").lower()

        with open(self.csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
            with self.output().open("w") as out:
                json.dump({"table": table_name, "rows_loaded": 0}, out)
            return

        columns = list(rows[0].keys())

        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # Create table if not exists (all columns as TEXT, matching Step Functions lambda)
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

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as out:
            json.dump({"table": table_name, "rows_loaded": len(rows)}, out)


# ---------------------------------------------------------------------------
# Task 2b: ProcessAllCSVs (dynamic fan-out)
# ---------------------------------------------------------------------------


class ProcessAllCSVs(luigi.Task):
    """
    Creates one LoadCSVToPostgres task per CSV discovered by UnzipFile.

    DIVERGENCE: Step Functions uses a Map state that receives the CSV list as
    input and fans out automatically. Luigi cannot dynamically create task
    dependencies based on upstream output at definition time — we must read
    the UnzipFile output in requires() and yield one LoadCSVToPostgres per CSV.
    Luigi evaluates requires() at scheduling time, so the UnzipFile task must
    complete before ProcessAllCSVs can be scheduled (this is handled
    automatically by the Luigi scheduler via the dependency on UnzipFile).
    """

    zip_path = luigi.Parameter()
    extract_dir = luigi.Parameter()
    run_id = luigi.Parameter()

    def requires(self):
        return UnzipFile(
            zip_path=self.zip_path,
            extract_dir=self.extract_dir,
            run_id=self.run_id,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "all_csvs_loaded.json")
        )

    def run(self):
        # Read the list of CSV paths from the UnzipFile output
        with self.input().open("r") as f:
            unzip_result = json.load(f)

        csv_paths = unzip_result["csv_paths"]

        # Yield individual load tasks — Luigi will schedule and run them
        # (potentially in parallel, depending on --workers)
        load_tasks = [
            LoadCSVToPostgres(csv_path=csv_path, run_id=self.run_id)
            for csv_path in csv_paths
        ]
        yield load_tasks

        # Collect results from all load tasks
        load_results = []
        for task in load_tasks:
            with task.output().open("r") as f:
                load_results.append(json.load(f))

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as out:
            json.dump({"load_results": load_results, "total_csvs": len(csv_paths)}, out)


# ---------------------------------------------------------------------------
# Task 3: RunSQLTransform
# ---------------------------------------------------------------------------


class RunSQLTransform(luigi.Task):
    """
    Runs the SQL transformation that joins loaded CSV tables into combined_report.

    DIVERGENCE: Step Functions provides Retry with IntervalSeconds=10,
    MaxAttempts=2, BackoffRate=2.0 for task failures. Luigi has no retry
    mechanism — if the SQL fails, the task fails permanently until re-run.
    """

    zip_path = luigi.Parameter()
    extract_dir = luigi.Parameter()
    run_id = luigi.Parameter()

    def requires(self):
        return ProcessAllCSVs(
            zip_path=self.zip_path,
            extract_dir=self.extract_dir,
            run_id=self.run_id,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(MARKER_DIR, self.run_id, "sql_transform_complete.json")
        )

    def run(self):
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

        os.makedirs(os.path.dirname(self.output().path), exist_ok=True)
        with self.output().open("w") as out:
            json.dump(
                {
                    "transform_result": {
                        "table": "combined_report",
                        "row_count": row_count,
                    }
                },
                out,
            )


# ---------------------------------------------------------------------------
# Task 4: ConvertToParquet
# ---------------------------------------------------------------------------


class ConvertToParquet(luigi.Task):
    """
    Reads the combined_report table from Postgres and writes it as a Parquet file.

    In Step Functions, the Parquet is uploaded to S3. Here we write to a local
    output directory.

    DIVERGENCE: Step Functions Retry for infrastructure errors (Lambda SDK exceptions)
    has no analog in Luigi. A connection failure here means the task fails permanently.
    """

    zip_path = luigi.Parameter()
    extract_dir = luigi.Parameter()
    run_id = luigi.Parameter()
    output_dir = luigi.Parameter(default="/tmp/luigi-output/dag1")

    def requires(self):
        return RunSQLTransform(
            zip_path=self.zip_path,
            extract_dir=self.extract_dir,
            run_id=self.run_id,
        )

    def output(self):
        return luigi.LocalTarget(
            os.path.join(self.output_dir, self.run_id, "combined_report.parquet")
        )

    def run(self):
        # Read the transform result to know which table to export
        with self.input().open("r") as f:
            transform_data = json.load(f)

        table_name = transform_data["transform_result"]["table"]

        conn = get_db_connection()
        try:
            # Read data into a list of dicts (avoids pandas dependency matching the lambda)
            with conn.cursor() as cur:
                cur.execute(f'SELECT * FROM "{table_name}"')
                columns = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
        finally:
            conn.close()

        # Build a PyArrow table and write Parquet
        if rows:
            col_arrays = {}
            for i, col in enumerate(columns):
                col_arrays[col] = [row[i] for row in rows]
            table = pa.table(col_arrays)
        else:
            table = pa.table({col: [] for col in columns})

        output_path = self.output().path
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        pq.write_table(table, output_path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    luigi.run()
