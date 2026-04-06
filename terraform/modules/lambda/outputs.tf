output "lambda_function_arn" {
  description = "ARN of the Lambda trigger function"
  value       = aws_lambda_function.trigger.arn
}