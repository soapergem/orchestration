# ---------------------------------------------------------------------------
# DAG 1: CSV ETL Pipeline
#
# UnzipFile (S3) -> ProcessCSVs (inline Map, concurrency 10, loads each CSV into
# Neon) -> RunSQLTransform (JOIN into combined_report) -> ConvertToParquet
# (reads combined_report, writes Parquet to S3).
#
# DB-touching lambdas get the psycopg2 layer + NEON_DB_PARAM; their db.py pins a
# dedicated `<BAKEOFF_NS>_dag1` schema so DAG 1's dynamic tables don't collide with the
# DAG 3/4 transactional tables in the same Neon database. convert_to_parquet
# also gets the pyarrow layer.
# ---------------------------------------------------------------------------

locals {
  dag1_src = "${path.module}/../../step-functions/dag1-csv-etl"

  dag1_lambdas = {
    unzip_file           = { handler = "unzip_file.handler", db = false, parquet = false, timeout = 120, mem = 512 }
    load_csv_to_postgres = { handler = "load_csv_to_postgres.handler", db = true, parquet = false, timeout = 120, mem = 512 }
    run_sql_transform    = { handler = "run_sql_transform.handler", db = true, parquet = false, timeout = 300, mem = 512 }
    convert_to_parquet   = { handler = "convert_to_parquet.handler", db = true, parquet = true, timeout = 300, mem = 1024 }
  }
}

data "archive_file" "dag1_lambdas" {
  type        = "zip"
  source_dir  = "${local.dag1_src}/lambdas"
  output_path = "${path.module}/build/dag1-lambdas.zip"
}

resource "aws_cloudwatch_log_group" "dag1" {
  for_each          = local.dag1_lambdas
  name              = "/aws/lambda/${var.name_prefix}-dag1-${each.key}"
  retention_in_days = 7
}

resource "aws_lambda_function" "dag1" {
  for_each = local.dag1_lambdas

  function_name    = "${var.name_prefix}-dag1-${each.key}"
  role             = aws_iam_role.lambda_exec.arn
  runtime          = var.lambda_runtime
  architectures    = [var.lambda_architecture]
  handler          = each.value.handler
  filename         = data.archive_file.dag1_lambdas.output_path
  source_code_hash = data.archive_file.dag1_lambdas.output_base64sha256
  timeout          = each.value.timeout
  memory_size      = each.value.mem

  layers = compact([
    each.value.db ? aws_lambda_layer_version.psycopg2.arn : "",
    each.value.parquet ? aws_lambda_layer_version.pyarrow.arn : "",
  ])

  environment {
    variables = {
      NEON_DB_PARAM = aws_ssm_parameter.neon_database_url.name
      BAKEOFF_NS    = var.bakeoff_ns
    }
  }

  depends_on = [aws_cloudwatch_log_group.dag1]
}

resource "aws_sfn_state_machine" "dag1" {
  name     = "${var.name_prefix}-dag1-csv-etl"
  role_arn = aws_iam_role.sfn_exec.arn

  definition = templatefile("${local.dag1_src}/state-machine.asl.json", {
    UnzipFileFunctionArn         = aws_lambda_function.dag1["unzip_file"].arn
    LoadCSVToPostgresFunctionArn = aws_lambda_function.dag1["load_csv_to_postgres"].arn
    RunSQLTransformFunctionArn   = aws_lambda_function.dag1["run_sql_transform"].arn
    ConvertToParquetFunctionArn  = aws_lambda_function.dag1["convert_to_parquet"].arn
  })
}
