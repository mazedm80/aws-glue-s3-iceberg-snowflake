data "aws_iam_policy_document" "glue_assume_role_policy" {
  statement {
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name               = "AWSGlueS3Table-${var.env}"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role_policy.json
}

resource "aws_iam_role_policy" "glue_policy" {
  name = "AWSGlueS3TablePolicy-${var.env}"
  role = aws_iam_role.glue_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect   = "Allow"
        Action   = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.bucket_name}",
          "arn:aws:s3:::${var.bucket_name}/*",
        ]
      },
      {
        Effect   = "Allow"
        Action   = [
          "s3tables:CreateTable",
          "s3tables:PutTableData",
          "s3tables:GetTableData",
          "s3tables:GetTableMetadataLocation",
          "s3tables:UpdateTableMetadataLocation",
          "s3tables:GetNamespace",
          "s3tables:CreateNamespace"
        ]
        Resource = [
            var.table_bucket_arn,
            "${var.table_bucket_arn}/table/*"
        ]
      },
      {
        Effect   = "Allow"
        Action   = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "*"
      }
    ]
  })
}