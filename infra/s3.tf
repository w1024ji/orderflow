resource "aws_s3_bucket" "orderflow_data" {
  bucket = var.s3_bucket_name

  lifecycle {
    prevent_destroy = true
  }
}