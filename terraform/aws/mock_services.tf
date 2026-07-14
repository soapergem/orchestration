# ---------------------------------------------------------------------------
# Mock services for DAG 2 (callback-fetch) and DAG 4 (approval, shipping).
#
# Images are built and pushed to ECR (scripts/build-push-mock-services.sh, arm64
# for K3s), then deployed to the K3s cluster (see shared-services/k8s/) in the
# `orchestrators` namespace and exposed at *.gemovationlabs.com.
#
# callback-fetch and approval call Step Functions SendTaskSuccess from OUTSIDE
# AWS, so they authenticate with a dedicated least-privilege IAM user's access
# key (delivered to the pods as a K8s Secret). shipping needs no AWS access.
# ---------------------------------------------------------------------------

locals {
  mock_services      = ["callback-fetch", "approval", "shipping"]
  callback_fetch_url = "https://callback-fetch.${var.mock_service_base_domain}"
  approval_url       = "https://approval.${var.mock_service_base_domain}"
  shipping_url       = "https://shipping.${var.mock_service_base_domain}"
}

resource "aws_ecr_repository" "mock" {
  for_each     = toset(local.mock_services)
  name         = "${var.name_prefix}-${each.key}"
  force_delete = true
}

# Dedicated IAM user for the SendTaskSuccess/Failure callback from K3s.
resource "aws_iam_user" "callback_resume" {
  name = "${var.name_prefix}-callback-resume"
}

data "aws_iam_policy_document" "callback_resume" {
  statement {
    sid = "ResumeTaskToken"
    # Task tokens can't be scoped to a state-machine ARN, so this is "*".
    actions   = ["states:SendTaskSuccess", "states:SendTaskFailure", "states:SendTaskHeartbeat"]
    resources = ["*"]
  }
}

resource "aws_iam_user_policy" "callback_resume" {
  name   = "${var.name_prefix}-callback-resume"
  user   = aws_iam_user.callback_resume.name
  policy = data.aws_iam_policy_document.callback_resume.json
}

resource "aws_iam_access_key" "callback_resume" {
  user = aws_iam_user.callback_resume.name
}
