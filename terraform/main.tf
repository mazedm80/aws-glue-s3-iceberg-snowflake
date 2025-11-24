terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

module "s3" {
  source     = "./modules/s3"
  env        = var.env
  aws_region = var.aws_region
}

module "glue" {
  source = "./modules/glue"
  env    = var.env
  bucket_name = module.s3.bucket_name
  table_bucket_arn = module.s3.table_bucket_arn
}