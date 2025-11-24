resource "aws_s3_bucket" "s3-airport-raw-dev" {
    bucket = "airport-raw-${var.aws_region}-${random_id.suffix.hex}-${var.env}"
    tags = {
        name = "S3 Airport Raw - ${var.env}"
        environment = var.env
    }
    force_destroy = true
}

resource "aws_s3tables_table_bucket" "s3-airport-stages-dev" {
    name = "airport-${var.aws_region}-${random_id.suffix.hex}-${var.env}"
    force_destroy = true
}

resource "random_id" "suffix" {
    byte_length = 4
}

variable "env" {
    description = "Deployment environment"
    type = string
}

variable "aws_region" {
  description = "The default region."
  type        = string
}
