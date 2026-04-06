output "glue_crawler_name" {
  description = "Name of the Glue crawler"
  value       = aws_glue_crawler.tradesphere_crawler.name
}