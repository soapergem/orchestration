# ---------------------------------------------------------------------------
# Mock services for DAG 2 (callback-fetch) and DAG 4 (approval, shipping).
#
# Images are built and pushed to ECR (scripts/build-push-mock-services.sh, arm64
# for K3s), then deployed to the K3s cluster (see shared-services/deploy/) in the
# `orchestrators` namespace and exposed at *.example.com.
#
# callback-fetch and approval call Step Functions SendTaskSuccess from OUTSIDE
# AWS, so they authenticate with a dedicated least-privilege IAM user's access
# key (delivered to the pods as a K8s Secret). shipping needs no AWS access.
# ---------------------------------------------------------------------------

locals {
  mock_services      = ["callback-fetch", "approval", "shipping"]
  mock_service_fqdn  = { for s in local.mock_services : s => "${var.mock_service_subdomain_prefix}${s}.${var.mock_service_base_domain}" }
  callback_fetch_url = "https://${local.mock_service_fqdn["callback-fetch"]}"
  approval_url       = "https://${local.mock_service_fqdn["approval"]}"
  shipping_url       = "https://${local.mock_service_fqdn["shipping"]}"
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

# Dedicated read-only IAM user for the in-cluster fixture-service, which fetches
# DAG 1's archive from S3 on boot (Kubernetes has no host bind mount, and the
# bucket blocks all public access). Scoped to the single object it needs -- unlike
# the callback user above, which cannot be narrowed because task tokens carry no
# ARN.
resource "aws_iam_user" "fixture_reader" {
  name = "${var.name_prefix}-fixture-reader"
}

data "aws_iam_policy_document" "fixture_reader" {
  statement {
    sid       = "ReadDag1Input"
    actions   = ["s3:GetObject"]
    resources = ["${aws_s3_bucket.dag1.arn}/input/*"]
  }
}

resource "aws_iam_user_policy" "fixture_reader" {
  name   = "${var.name_prefix}-fixture-reader"
  user   = aws_iam_user.fixture_reader.name
  policy = data.aws_iam_policy_document.fixture_reader.json
}

resource "aws_iam_access_key" "fixture_reader" {
  user = aws_iam_user.fixture_reader.name
}
