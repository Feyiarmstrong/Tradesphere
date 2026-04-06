variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name used for resource naming"
  type        = string
  default     = "tradesphere"
}

variable "name_suffix" {
  description = "Unique suffix for resource names"
  type        = string
  default     = "feyisayo"
}

variable "alert_email" {
  description = "Email address for SNS alerts"
  type        = string
  default     = "solapeajiboye@gmail.com"
}