output "athena_database" {
  value = aws_glue_catalog_database.orderflow.name
}

output "athena_table" {
  value = aws_glue_catalog_table.imbalance.name
}

output "s3_bucket" {
  value = aws_s3_bucket.orderflow_data.id
}