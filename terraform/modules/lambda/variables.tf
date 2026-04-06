variable "project_name" {
  description = "Project name"
  type        = string
}

variable "name_suffix" {
  description = "Unique suffix for resource names"
  type        = string
}

variable "raw_bucket_name" {
  description = "Name of the raw S3 bucket"
  type        = string
}

variable "sns_topic_arn" {
  description = "ARN of the SNS alerts topic"
  type        = string
}