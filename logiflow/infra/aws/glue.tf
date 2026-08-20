resource "aws_glue_catalog_database" "logiflow" {
  name        = "${var.project_name}_catalog"
  description = "LogiFlow data lake catalog -- tables inferred from S3 by the crawler"
}

# ── IAM role the crawler assumes ─────────────────────────────────────
resource "aws_iam_role" "glue_crawler" {
  name = "${var.project_name}-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action    = "sts:AssumeRole"
      Effect    = "Allow"
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
}

# AWS-managed policy covering the baseline Glue permissions
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_crawler.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

# Scoped to OUR bucket only -- the crawler can't read anything else in the account
resource "aws_iam_role_policy" "glue_s3_access" {
  name = "${var.project_name}-glue-s3-access"
  role = aws_iam_role.glue_crawler.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = ["s3:GetObject", "s3:ListBucket"]
      Resource = [
        aws_s3_bucket.datalake.arn,
        "${aws_s3_bucket.datalake.arn}/*",
      ]
    }]
  })
}

# ── The crawler itself ───────────────────────────────────────────────
# Scans s3://<bucket>/raw/, infers the schema, and registers a table
# in the Glue catalog that Athena can then query with plain SQL.
resource "aws_glue_crawler" "shipments" {
  name          = "${var.project_name}-shipments-crawler"
  database_name = aws_glue_catalog_database.logiflow.name
  role          = aws_iam_role.glue_crawler.arn

  s3_target {
    path = "s3://${aws_s3_bucket.datalake.bucket}/raw/"
  }
}