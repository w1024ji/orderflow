variable "project_name" {
  description = "Project name prefix"
  type        = string
  default     = "orderflow"
}

variable "s3_bucket_name" {
  description = "Existing S3 bucket name"
  type        = string
  default     = "orderflow-data"
}

variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "iam_user_name" {
  description = "Existing IAM user name used by Flink for S3 access"
  type        = string
}

