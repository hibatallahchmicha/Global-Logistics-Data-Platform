# ── Data lake bucket (replaces MinIO) ────────────────────────────────
resource "aws_s3_bucket" "datalake" {
  bucket = local.datalake_bucket

  # Allows `terraform destroy` to remove the bucket even when it still
  # contains objects. Correct for a teardown-friendly demo environment;
  # would be dangerous in production.
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# ── Athena query results bucket ──────────────────────────────────────
# Athena requires an S3 location to write results to; it cannot run without one.
resource "aws_s3_bucket" "athena_results" {
  bucket        = local.results_bucket
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}