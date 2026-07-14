# ---------------------------------------------------------------------------
# DAG 3: Payment Processing
#
# 5 lambdas (packaged from the shared source dir) + a Step Functions state
# machine whose ASL is templated with the resolved lambda ARNs. DB-touching
# lambdas get the psycopg2 layer and the NEON_DB_PARAM env var pointing at the
# SSM SecureString.
# ---------------------------------------------------------------------------

locals {
  dag3_src = "${path.module}/../../step-functions/dag3-payment"

  # handler -> whether it connects to Neon (needs the psycopg2 layer + DSN)
  dag3_lambdas = {
    validate_payment       = { handler = "validate_payment.handler", db = true }
    process_payment        = { handler = "process_payment.handler", db = false }
    update_database        = { handler = "update_database.handler", db = true }
    send_notification      = { handler = "send_notification.handler", db = false }
    handle_payment_failure = { handler = "handle_payment_failure.handler", db = true }
  }
}

# One deployment package for all DAG 3 handlers; each function selects its own
# handler entrypoint from it.
data "archive_file" "dag3_lambdas" {
  type        = "zip"
  source_dir  = "${local.dag3_src}/lambdas"
  output_path = "${path.module}/build/dag3-lambdas.zip"
}

resource "aws_cloudwatch_log_group" "dag3" {
  for_each          = local.dag3_lambdas
  name              = "/aws/lambda/${var.name_prefix}-dag3-${each.key}"
  retention_in_days = 7
}

resource "aws_lambda_function" "dag3" {
  for_each = local.dag3_lambdas

  function_name    = "${var.name_prefix}-dag3-${each.key}"
  role             = aws_iam_role.lambda_exec.arn
  runtime          = var.lambda_runtime
  architectures    = [var.lambda_architecture]
  handler          = each.value.handler
  filename         = data.archive_file.dag3_lambdas.output_path
  source_code_hash = data.archive_file.dag3_lambdas.output_base64sha256
  timeout          = 60
  memory_size      = 256

  layers = each.value.db ? [aws_lambda_layer_version.psycopg2.arn] : []

  environment {
    variables = {
      NEON_DB_PARAM = aws_ssm_parameter.neon_database_url.name
    }
  }

  depends_on = [aws_cloudwatch_log_group.dag3]
}

resource "aws_sfn_state_machine" "dag3" {
  name     = "${var.name_prefix}-dag3-payment"
  role_arn = aws_iam_role.sfn_exec.arn

  definition = templatefile("${local.dag3_src}/state-machine.asl.json", {
    ValidatePaymentFunctionArn      = aws_lambda_function.dag3["validate_payment"].arn
    ProcessPaymentFunctionArn       = aws_lambda_function.dag3["process_payment"].arn
    UpdateDatabaseFunctionArn       = aws_lambda_function.dag3["update_database"].arn
    SendNotificationFunctionArn     = aws_lambda_function.dag3["send_notification"].arn
    HandlePaymentFailureFunctionArn = aws_lambda_function.dag3["handle_payment_failure"].arn
  })
}
