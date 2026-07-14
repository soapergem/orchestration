# ---------------------------------------------------------------------------
# DAG 2: API Fan-Out with Async Callback
#
# SubmitAsyncFetch (.waitForTaskToken) registers with the callback-fetch service
# and suspends; the service resumes it via SendTaskSuccess. Then ProcessFetchResult
# -> CheckItemsExist (Choice) -> FanOutAPIRequests (Map, concurrency 20) ->
# CombineResults. No DB, no layers (urllib3 + boto3 are in the runtime).
# ---------------------------------------------------------------------------

locals {
  dag2_src = "${path.module}/../../step-functions/dag2-api-fanout"

  dag2_lambdas = {
    submit_async_fetch   = "submit_async_fetch.handler"
    process_fetch_result = "process_fetch_result.handler"
    fetch_item_detail    = "fetch_item_detail.handler"
    combine_results      = "combine_results.handler"
  }
}

data "archive_file" "dag2_lambdas" {
  type        = "zip"
  source_dir  = "${local.dag2_src}/lambdas"
  output_path = "${path.module}/build/dag2-lambdas.zip"
}

resource "aws_cloudwatch_log_group" "dag2" {
  for_each          = local.dag2_lambdas
  name              = "/aws/lambda/${var.name_prefix}-dag2-${each.key}"
  retention_in_days = 7
}

resource "aws_lambda_function" "dag2" {
  for_each = local.dag2_lambdas

  function_name    = "${var.name_prefix}-dag2-${each.key}"
  role             = aws_iam_role.lambda_exec.arn
  runtime          = var.lambda_runtime
  architectures    = [var.lambda_architecture]
  handler          = each.value
  filename         = data.archive_file.dag2_lambdas.output_path
  source_code_hash = data.archive_file.dag2_lambdas.output_base64sha256
  timeout          = 60
  memory_size      = 256

  # Only submit_async_fetch needs the service URL; harmless on the others.
  # Points at the callback-fetch service running on K3s.
  environment {
    variables = {
      CALLBACK_FETCH_SERVICE_URL = local.callback_fetch_url
    }
  }

  depends_on = [aws_cloudwatch_log_group.dag2]
}

resource "aws_sfn_state_machine" "dag2" {
  name     = "${var.name_prefix}-dag2-api-fanout"
  role_arn = aws_iam_role.sfn_exec.arn

  definition = templatefile("${local.dag2_src}/state-machine.asl.json", {
    SubmitAsyncFetchFunctionArn   = aws_lambda_function.dag2["submit_async_fetch"].arn
    ProcessFetchResultFunctionArn = aws_lambda_function.dag2["process_fetch_result"].arn
    FetchItemDetailFunctionArn    = aws_lambda_function.dag2["fetch_item_detail"].arn
    CombineResultsFunctionArn     = aws_lambda_function.dag2["combine_results"].arn
  })
}
