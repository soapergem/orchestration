# psycopg2 Lambda layer. Built by scripts/build-psycopg2-layer.sh, which
# pip-installs psycopg2-binary for the Lambda platform into build/psycopg2-layer/
# (run it before `terraform apply`). The DB-touching lambdas attach this layer.
data "archive_file" "psycopg2_layer" {
  type        = "zip"
  source_dir  = "${path.module}/build/psycopg2-layer"
  output_path = "${path.module}/build/psycopg2-layer.zip"
}

resource "aws_lambda_layer_version" "psycopg2" {
  layer_name               = "${var.name_prefix}-psycopg2"
  description              = "psycopg2-binary for ${var.lambda_runtime} / ${var.lambda_architecture}"
  filename                 = data.archive_file.psycopg2_layer.output_path
  source_code_hash         = data.archive_file.psycopg2_layer.output_base64sha256
  compatible_runtimes      = [var.lambda_runtime]
  compatible_architectures = [var.lambda_architecture]
}

# pyarrow Lambda layer for DAG 1's convert_to_parquet. Built by
# scripts/build-pyarrow-layer.sh into build/pyarrow-layer/ (~140 MB unzipped;
# stacks with the psycopg2 layer well under the 250 MB limit).
data "archive_file" "pyarrow_layer" {
  type        = "zip"
  source_dir  = "${path.module}/build/pyarrow-layer"
  output_path = "${path.module}/build/pyarrow-layer.zip"
}

resource "aws_lambda_layer_version" "pyarrow" {
  layer_name               = "${var.name_prefix}-pyarrow"
  description              = "pyarrow for ${var.lambda_runtime} / ${var.lambda_architecture}"
  filename                 = data.archive_file.pyarrow_layer.output_path
  source_code_hash         = data.archive_file.pyarrow_layer.output_base64sha256
  compatible_runtimes      = [var.lambda_runtime]
  compatible_architectures = [var.lambda_architecture]
}
