output "datalake_bucket" {
  description = "S3 bucket replacing MinIO as the raw landing zone"
  value       = aws_s3_bucket.datalake.bucket
}

output "glue_database" {
  description = "Glue catalog database name"
  value       = aws_glue_catalog_database.logiflow.name
}

output "glue_crawler" {
  description = "Run this crawler after uploading data to register the tables"
  value       = aws_glue_crawler.shipments.name
}

output "athena_workgroup" {
  description = "Select this workgroup in the Athena console"
  value       = aws_athena_workgroup.logiflow.name
}