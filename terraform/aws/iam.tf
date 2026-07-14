# ---------------------------------------------------------------------------
# Lambda execution role: CloudWatch Logs + read the one Neon SSM parameter.
# No VPC, so no ENI permissions are needed (Neon is reached over the public
# internet with TLS).
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "lambda_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda_exec" {
  name               = "${var.name_prefix}-lambda-exec"
  assume_role_policy = data.aws_iam_policy_document.lambda_assume.json
}

resource "aws_iam_role_policy_attachment" "lambda_logs" {
  role       = aws_iam_role.lambda_exec.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

data "aws_iam_policy_document" "lambda_ssm" {
  statement {
    sid       = "ReadNeonDsn"
    actions   = ["ssm:GetParameter"]
    resources = [aws_ssm_parameter.neon_database_url.arn]
  }
}

resource "aws_iam_role_policy" "lambda_ssm" {
  name   = "${var.name_prefix}-lambda-ssm"
  role   = aws_iam_role.lambda_exec.id
  policy = data.aws_iam_policy_document.lambda_ssm.json
}

# DAG 1 lambdas read the ZIP / write CSVs + Parquet in the DAG 1 bucket.
data "aws_iam_policy_document" "lambda_s3" {
  statement {
    sid       = "Dag1ObjectRW"
    actions   = ["s3:GetObject", "s3:PutObject"]
    resources = ["${aws_s3_bucket.dag1.arn}/*"]
  }
  statement {
    sid       = "Dag1BucketList"
    actions   = ["s3:ListBucket"]
    resources = [aws_s3_bucket.dag1.arn]
  }
}

resource "aws_iam_role_policy" "lambda_s3" {
  name   = "${var.name_prefix}-lambda-s3"
  role   = aws_iam_role.lambda_exec.id
  policy = data.aws_iam_policy_document.lambda_s3.json
}

# ---------------------------------------------------------------------------
# Step Functions execution role: invoke the DAG lambdas.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "sfn_assume" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn_exec" {
  name               = "${var.name_prefix}-sfn-exec"
  assume_role_policy = data.aws_iam_policy_document.sfn_assume.json
}

data "aws_iam_policy_document" "sfn_invoke" {
  statement {
    sid     = "InvokeDagLambdas"
    actions = ["lambda:InvokeFunction"]
    resources = concat(
      [for fn in aws_lambda_function.dag3 : fn.arn],
      [for fn in aws_lambda_function.dag1 : fn.arn],
      [for fn in aws_lambda_function.dag2 : fn.arn],
      [for fn in aws_lambda_function.dag4 : fn.arn],
    )
  }
}

# DAG 4 main state machine invokes the three sub-workflows via
# startExecution.sync:2, which needs StartExecution on the child machines plus
# DescribeExecution/StopExecution on their executions and the EventBridge
# managed rule that .sync relies on.
data "aws_iam_policy_document" "sfn_child_exec" {
  statement {
    sid     = "StartChildWorkflows"
    actions = ["states:StartExecution"]
    resources = [
      aws_sfn_state_machine.dag4_reserve_inventory.arn,
      aws_sfn_state_machine.dag4_manager_approval.arn,
      aws_sfn_state_machine.dag4_shipping.arn,
    ]
  }
  statement {
    sid     = "SyncChildWorkflows"
    actions = ["states:DescribeExecution", "states:StopExecution"]
    resources = [
      "${replace(aws_sfn_state_machine.dag4_reserve_inventory.arn, ":stateMachine:", ":execution:")}:*",
      "${replace(aws_sfn_state_machine.dag4_manager_approval.arn, ":stateMachine:", ":execution:")}:*",
      "${replace(aws_sfn_state_machine.dag4_shipping.arn, ":stateMachine:", ":execution:")}:*",
    ]
  }
  statement {
    sid       = "SyncEventBridge"
    actions   = ["events:PutTargets", "events:PutRule", "events:DescribeRule"]
    resources = ["arn:aws:events:${var.aws_region}:${data.aws_caller_identity.current.account_id}:rule/StepFunctionsGetEventsForStepFunctionsExecutionRule"]
  }
}

resource "aws_iam_role_policy" "sfn_child_exec" {
  name   = "${var.name_prefix}-sfn-child-exec"
  role   = aws_iam_role.sfn_exec.id
  policy = data.aws_iam_policy_document.sfn_child_exec.json
}

resource "aws_iam_role_policy" "sfn_invoke" {
  name   = "${var.name_prefix}-sfn-invoke"
  role   = aws_iam_role.sfn_exec.id
  policy = data.aws_iam_policy_document.sfn_invoke.json
}
