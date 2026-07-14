data "aws_caller_identity" "current" {}

# Bucket for DAG 1 file I/O: ZIP input and Parquet output.
resource "aws_s3_bucket" "dag1" {
  bucket        = "${var.name_prefix}-dag1-${data.aws_caller_identity.current.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "dag1" {
  bucket                  = aws_s3_bucket.dag1.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Seed the sample ZIP so DAG 1 is runnable immediately after apply.
resource "aws_s3_object" "sample_zip" {
  bucket = aws_s3_bucket.dag1.id
  key    = "input/sample-data.zip"
  source = "${path.module}/../../test-data/sample-data.zip"
  etag   = filemd5("${path.module}/../../test-data/sample-data.zip")
}
