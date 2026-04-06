terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

module "s3" {
  source      = "./modules/s3"
  project_name = var.project_name
  name_suffix  = var.name_suffix
}

module "sns" {
  source       = "./modules/sns"
  project_name = var.project_name
  alert_email  = var.alert_email
}

module "lambda" {
  source           = "./modules/lambda"
  project_name     = var.project_name
  name_suffix      = var.name_suffix
  raw_bucket_name  = module.s3.raw_bucket_name
  sns_topic_arn    = module.sns.sns_topic_arn
}

# module "glue" {
#   source          = "./modules/glue"
#   project_name    = var.project_name
#   name_suffix     = var.name_suffix
#   raw_bucket_name = module.s3.raw_bucket_name
# }