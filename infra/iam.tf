resource "aws_iam_policy" "flink_s3_access" {
  name        = "${var.project_name}-flink-s3-access"
  description = "Allows Flink to read/write orderflow-data S3 bucket"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket",
          "s3:DeleteObject"
        ]
        Resource = [
          aws_s3_bucket.orderflow_data.arn,
          "${aws_s3_bucket.orderflow_data.arn}/*"
        ]
      }
    ]
  })
}

resource "aws_iam_user_policy_attachment" "flink_s3_attach" {
  user       = var.iam_user_name
  policy_arn = aws_iam_policy.flink_s3_access.arn
}