# ---------------------------------------------------------------------------
# DAG 4: Order Fulfillment with human approval + saga compensation
#
# Main state machine invokes three sub-workflow state machines via
# startExecution.sync:2 (ReserveInventory, ManagerApproval, Shipping) and owns
# the compensation path. ManagerApproval suspends on .waitForTaskToken until the
# approval service (K3s) resumes it; Shipping calls the shipping service (K3s).
# DB lambdas use the shared db.py (SSM DSN, public schema). No layers beyond
# psycopg2; urllib3 is in the runtime.
# ---------------------------------------------------------------------------

locals {
  dag4_src = "${path.module}/../../step-functions/dag4-order-fulfillment"

  dag4_lambdas = {
    validate_order           = { handler = "validate_order.handler", db = true, approval = false, shipping = false }
    reserve_inventory        = { handler = "reserve_inventory.handler", db = true, approval = false, shipping = false }
    release_inventory        = { handler = "release_inventory.handler", db = true, approval = false, shipping = false }
    update_order_status      = { handler = "update_order_status.handler", db = true, approval = false, shipping = false }
    record_approval_decision = { handler = "record_approval_decision.handler", db = true, approval = false, shipping = false }
    request_approval         = { handler = "request_approval.handler", db = true, approval = true, shipping = false }
    call_shipping_api        = { handler = "call_shipping_api.handler", db = false, approval = false, shipping = true }
    send_order_notification  = { handler = "send_order_notification.handler", db = false, approval = false, shipping = false }
  }

  dag4_env = { for k, v in local.dag4_lambdas : k => merge(
    v.db ? { NEON_DB_PARAM = aws_ssm_parameter.neon_database_url.name } : {},
    v.approval ? { APPROVAL_SERVICE_URL = local.approval_url } : {},
    v.shipping ? { SHIPPING_SERVICE_URL = local.shipping_url } : {},
  ) }
}

data "archive_file" "dag4_lambdas" {
  type        = "zip"
  source_dir  = "${local.dag4_src}/lambdas"
  output_path = "${path.module}/build/dag4-lambdas.zip"
}

resource "aws_cloudwatch_log_group" "dag4" {
  for_each          = local.dag4_lambdas
  name              = "/aws/lambda/${var.name_prefix}-dag4-${each.key}"
  retention_in_days = 7
}

resource "aws_lambda_function" "dag4" {
  for_each = local.dag4_lambdas

  function_name    = "${var.name_prefix}-dag4-${each.key}"
  role             = aws_iam_role.lambda_exec.arn
  runtime          = var.lambda_runtime
  architectures    = [var.lambda_architecture]
  handler          = each.value.handler
  filename         = data.archive_file.dag4_lambdas.output_path
  source_code_hash = data.archive_file.dag4_lambdas.output_base64sha256
  timeout          = 60
  memory_size      = 256

  layers = each.value.db ? [aws_lambda_layer_version.psycopg2.arn] : []

  dynamic "environment" {
    for_each = length(local.dag4_env[each.key]) > 0 ? [1] : []
    content {
      variables = local.dag4_env[each.key]
    }
  }

  depends_on = [aws_cloudwatch_log_group.dag4]
}

# --- Sub-workflow state machines ---

resource "aws_sfn_state_machine" "dag4_reserve_inventory" {
  name     = "${var.name_prefix}-dag4-reserve-inventory"
  role_arn = aws_iam_role.sfn_exec.arn
  definition = templatefile("${local.dag4_src}/sub-workflows/reserve-inventory.asl.json", {
    ReserveInventoryFunctionArn = aws_lambda_function.dag4["reserve_inventory"].arn
  })
}

resource "aws_sfn_state_machine" "dag4_manager_approval" {
  name     = "${var.name_prefix}-dag4-manager-approval"
  role_arn = aws_iam_role.sfn_exec.arn
  definition = templatefile("${local.dag4_src}/sub-workflows/manager-approval.asl.json", {
    RequestApprovalFunctionArn        = aws_lambda_function.dag4["request_approval"].arn
    RecordApprovalDecisionFunctionArn = aws_lambda_function.dag4["record_approval_decision"].arn
  })
}

resource "aws_sfn_state_machine" "dag4_shipping" {
  name     = "${var.name_prefix}-dag4-shipping"
  role_arn = aws_iam_role.sfn_exec.arn
  definition = templatefile("${local.dag4_src}/sub-workflows/shipping.asl.json", {
    CallShippingAPIFunctionArn = aws_lambda_function.dag4["call_shipping_api"].arn
  })
}

# --- Main state machine ---

resource "aws_sfn_state_machine" "dag4" {
  name     = "${var.name_prefix}-dag4-order-fulfillment"
  role_arn = aws_iam_role.sfn_exec.arn
  definition = templatefile("${local.dag4_src}/state-machine.asl.json", {
    ValidateOrderFunctionArn         = aws_lambda_function.dag4["validate_order"].arn
    ReleaseInventoryFunctionArn      = aws_lambda_function.dag4["release_inventory"].arn
    UpdateOrderStatusFunctionArn     = aws_lambda_function.dag4["update_order_status"].arn
    SendOrderNotificationFunctionArn = aws_lambda_function.dag4["send_order_notification"].arn
    ReserveInventoryStateMachineArn  = aws_sfn_state_machine.dag4_reserve_inventory.arn
    ManagerApprovalStateMachineArn   = aws_sfn_state_machine.dag4_manager_approval.arn
    ShippingStateMachineArn          = aws_sfn_state_machine.dag4_shipping.arn
  })
}
