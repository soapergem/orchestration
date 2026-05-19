"""
DAG 1: CSV ETL Pipeline -- Temporal Workflow

Unzip -> parallel CSV load -> SQL transform -> Parquet conversion.

Uses asyncio.gather() for fan-out parallelism and RetryPolicy for resilience.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy

with workflow.unsafe.imports_passed_through():
    from activities import (
        ConvertToParquetInput,
        ConvertToParquetOutput,
        LoadCSVInput,
        LoadCSVOutput,
        SQLTransformOutput,
        UnzipInput,
        UnzipOutput,
        convert_to_parquet,
        load_csv_to_postgres,
        run_sql_transform,
        unzip_file,
    )


# Shared retry policy: 3 attempts, 5s initial interval, 2x backoff
ETL_RETRY_POLICY = RetryPolicy(
    maximum_attempts=3,
    initial_interval=timedelta(seconds=5),
    backoff_coefficient=2.0,
)


@dataclass
class CSVETLInput:
    """Input for the CSV ETL pipeline."""
    zip_file_path: str
    extract_dir: str = "/tmp/extracted"
    output_path: str = "/tmp/output"


@dataclass
class CSVETLOutput:
    """Final output of the CSV ETL pipeline."""
    csv_files_loaded: int
    load_results: list[dict]
    transform_table: str
    transform_row_count: int
    parquet_path: str
    parquet_row_count: int


@workflow.defn
class CSVETLWorkflow:
    """
    CSV ETL Pipeline workflow.

    1. UnzipFile -- extract CSVs from a ZIP archive
    2. LoadCSVToPostgres -- fan out with asyncio.gather() over all CSVs
    3. RunSQLTransform -- SQL JOIN to produce combined_report
    4. ConvertToParquet -- read from DB, write Parquet file
    """

    @workflow.run
    async def run(self, input: CSVETLInput) -> CSVETLOutput:
        workflow.logger.info(
            "Starting CSV ETL pipeline for %s", input.zip_file_path
        )

        # Step 1: Unzip the file to extract CSV paths
        unzip_result: UnzipOutput = await workflow.execute_activity(
            unzip_file,
            UnzipInput(
                zip_file_path=input.zip_file_path,
                extract_dir=input.extract_dir,
            ),
            start_to_close_timeout=timedelta(minutes=2),
            retry_policy=ETL_RETRY_POLICY,
        )

        workflow.logger.info(
            "Extracted %d CSV files: %s",
            len(unzip_result.csv_paths),
            unzip_result.csv_paths,
        )

        if not unzip_result.csv_paths:
            raise workflow.ApplicationError(
                "No CSV files found in the archive",
                type="NoCSVFiles",
            )

        # Step 2: Fan out -- load each CSV into Postgres in parallel
        load_tasks = [
            workflow.execute_activity(
                load_csv_to_postgres,
                LoadCSVInput(csv_path=csv_path),
                start_to_close_timeout=timedelta(minutes=2),
                retry_policy=ETL_RETRY_POLICY,
            )
            for csv_path in unzip_result.csv_paths
        ]

        load_results: list[LoadCSVOutput] = await asyncio.gather(*load_tasks)

        workflow.logger.info(
            "Loaded %d CSV files into Postgres: %s",
            len(load_results),
            [(r.table, r.rows_loaded) for r in load_results],
        )

        # Step 3: Run SQL transform (JOIN across loaded tables)
        transform_result: SQLTransformOutput = await workflow.execute_activity(
            run_sql_transform,
            start_to_close_timeout=timedelta(minutes=15),
            retry_policy=ETL_RETRY_POLICY,
        )

        workflow.logger.info(
            "SQL transform complete: %d rows in '%s'",
            transform_result.row_count,
            transform_result.table,
        )

        # Step 4: Convert the transformed table to Parquet
        parquet_result: ConvertToParquetOutput = await workflow.execute_activity(
            convert_to_parquet,
            ConvertToParquetInput(
                table=transform_result.table,
                output_path=input.output_path,
            ),
            start_to_close_timeout=timedelta(minutes=10),
            retry_policy=ETL_RETRY_POLICY,
        )

        workflow.logger.info(
            "Parquet conversion complete: %d rows at %s",
            parquet_result.row_count,
            parquet_result.parquet_path,
        )

        return CSVETLOutput(
            csv_files_loaded=len(load_results),
            load_results=[
                {"table": r.table, "rows_loaded": r.rows_loaded}
                for r in load_results
            ],
            transform_table=transform_result.table,
            transform_row_count=transform_result.row_count,
            parquet_path=parquet_result.parquet_path,
            parquet_row_count=parquet_result.row_count,
        )
