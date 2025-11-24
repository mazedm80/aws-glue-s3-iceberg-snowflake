variable "glue_jobs" {
  description = "Map of Glue jobs to create"
  type = map(object({
    description = string
    script_name = string
    namespace   = string
    timeout     = number
  }))
  default = {
    us_flights_raw_to_silver = {
      description = "Read US Flights CSV data from S3 and write it in S3 table bucket."
      script_name = "us_flights_raw_to_silver.py"
      namespace   = "silver"
      timeout     = 600
    }
    cancelled_diverted_raw_to_silver = {
      description = "Read US Flights Cancelled Diverted CSV data from S3 and write it in S3 table bucket."
      script_name = "cancelled_diverted_raw_to_silver.py"
      namespace   = "silver"
      timeout     = 600
    }
    airport_weather_raw_to_silver = {
      description = "Read US Airport Weather CSV data from S3 and write it in S3 table bucket."
      script_name = "airport_weather_raw_to_silver.py"
      namespace   = "silver"
      timeout     = 600
    }
    airport_geolocation_raw_to_silver = {
      description = "Read US Airport Geolocation CSV data from S3 and write it in S3 table bucket."
      script_name = "airport_geolocation_raw_to_silver.py"
      namespace   = "silver"
      timeout     = 600
    }
    flight_performance_silver_gold = {
      description = "Transform Silver tables to Gold tables for Flight Performance analysis."
      script_name = "flight_performance_silver_gold.py"
      namespace   = "gold"
      timeout     = 600
    }
  }
}

resource "aws_glue_job" "jobs" {
  for_each = var.glue_jobs
  
  name         = each.key
  description  = each.value.description
  role_arn     = aws_iam_role.glue_role.arn
  glue_version = "5.0"
  max_retries  = 0
  timeout      = each.value.timeout
  number_of_workers = 2
  worker_type  = "G.1X"
  execution_class = "FLEX"

  command {
    script_location = "s3://${var.bucket_name}/scripts/${each.value.script_name}"
    python_version = "3"
    name = "glueetl"
  }

  default_arguments = {
    "--job-language" = "python"
    "--enable-metrics" = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--job-bookmark-option" = "job-bookmark-disable"
    "--JOB_NAME" = each.key
    "--datalake-formats" = "iceberg"
    "--extra-jars" = "s3://${var.bucket_name}/scripts/s3-tables-catalog-for-iceberg-runtime-0.1.5.jar"
    "--input_bucket" = var.bucket_name
    "--output_bucket_arn" = var.table_bucket_arn
    "--namespace" = each.value.namespace
    "--env" = "prod"
  }

  tags = {
    Name        = each.key
    Environment = var.env
  }
}

variable "env" {
    description = "Deployment environment"
    type = string
}

variable "bucket_name" {
  description = "S3 bucket name used by Glue jobs"
  type        = string
}

variable "table_bucket_arn" {
  description = "S3 table bucket ARN used by Glue jobs"
  type        = string
}