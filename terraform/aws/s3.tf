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

# Both fixture data files live here. They are BUILD ARTEFACTS -- gitignored, not
# baked into any image -- so S3 is the only copy a deployed environment can reach
# (Kubernetes has no host bind mount, and a ConfigMap cannot carry either of them:
# the corpus is megabytes, past the 1 MiB limit).
#
# Generate them before `terraform apply`:
#
#   uv run --no-project test-data/make-sample-data.py
#   uv run --no-project shared-services/fixture-service/build_dataset.py
#
# `count` on fileexists() rather than an unconditional upload: the corpus takes
# about an hour to build, so a plan must not hard-fail just because it is absent.
# Whatever is present gets uploaded; anything missing is simply not offered, and
# fixture-service degrades with an explanatory 503 rather than crashing.
locals {
  sample_zip_path = "${path.module}/../../test-data/sample-data.zip"
  books_path      = "${path.module}/../../test-data/books.json.gz"
}

# DAG 1's input archive. Byte-stable, so regenerating does not churn the etag.
resource "aws_s3_object" "sample_zip" {
  count  = fileexists(local.sample_zip_path) ? 1 : 0
  bucket = aws_s3_bucket.dag1.id
  key    = "input/sample-data.zip"
  source = local.sample_zip_path
  # try(): filemd5 is evaluated even when count is 0, so a missing file would
  # fail validate rather than simply skipping the upload.
  etag = try(filemd5(local.sample_zip_path), null)
}

# DAG 2's Open Library corpus (CC0 1.0), served by fixture-service.
resource "aws_s3_object" "books_corpus" {
  count  = fileexists(local.books_path) ? 1 : 0
  bucket = aws_s3_bucket.dag1.id
  key    = "input/books.json.gz"
  source = local.books_path
  etag   = try(filemd5(local.books_path), null)
}
