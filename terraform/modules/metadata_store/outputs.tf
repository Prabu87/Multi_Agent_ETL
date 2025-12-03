output "db_endpoint" {
  description = "Database endpoint"
  value       = aws_db_instance.metadata.endpoint
}

output "db_name" {
  description = "Database name"
  value       = aws_db_instance.metadata.db_name
}

output "db_password_secret_arn" {
  description = "ARN of the secret containing the database password"
  value       = aws_secretsmanager_secret.db_password.arn
}
