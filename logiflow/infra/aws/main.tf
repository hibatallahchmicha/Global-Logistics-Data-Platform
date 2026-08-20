terraform {
  required_version = ">= 1.5"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  # Every resource created here gets these tags automatically.
  # Makes cost attribution and "did I really delete everything?" easy.
  default_tags {
    tags = {
      Project   = "LogiFlow"
      ManagedBy = "Terraform"
    }
  }
}

data "aws_caller_identity" "current" {}

locals {
  # S3 bucket names must be globally unique across all of AWS.
  # Appending the account ID guarantees uniqueness deterministically
  # (no random suffix that changes between runs).
  datalake_bucket = "${var.project_name}-datalake-${data.aws_caller_identity.current.account_id}"
  results_bucket  = "${var.project_name}-athena-results-${data.aws_caller_identity.current.account_id}"
}