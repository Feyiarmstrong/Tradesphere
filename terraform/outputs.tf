output "raw_bucket_name" {
  description = "Name of the raw data S3 bucket"
  value       = module.s3.raw_bucket_name
}

output "tfstate_bucket_name" {
  description = "Name of the Terraform state S3 bucket"
  value       = module.s3.tfstate_bucket_name
}

output "sns_topic_arn" {
  description = "ARN of the SNS alerts topic"
  value       = module.sns.sns_topic_arn
}

output "lambda_function_arn" {
  description = "ARN of the Lambda trigger function"
  value       = module.lambda.lambda_function_arn
}

# output "glue_crawler_name" {
#   description = "Name of the Glue crawler"
#   value       = module.glue.glue_crawler_name
# }