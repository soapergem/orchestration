terraform {
  required_version = ">= 1.9"

  # PARTIAL backend configuration: the block is deliberately empty so that no
  # bucket name, key or region is committed. Every setting is supplied at init
  # time from a gitignored file:
  #
  #   cp backend.hcl.example backend.hcl     # then edit
  #   terraform -chdir=terraform/aws init -backend-config=backend.hcl
  #
  # or inline, if you would rather not keep a file at all:
  #
  #   terraform -chdir=terraform/aws init \
  #     -backend-config="bucket=$TF_STATE_BUCKET" \
  #     -backend-config="key=orchestration-bakeoff/aws.tfstate" \
  #     -backend-config="region=us-east-1"
  #
  # State holds the Neon DSN and other secrets in plaintext (see ssm.tf), so the
  # bucket must have encryption and versioning on, and must not be public.
  backend "s3" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.60"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4"
    }
  }
}
