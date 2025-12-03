output "vpc_id" {
  description = "ID of the VPC"
  value       = module.networking.vpc_id
}

output "message_bus_endpoint" {
  description = "Endpoint for the message bus"
  value       = module.message_bus.endpoint
  sensitive   = true
}

output "metadata_db_endpoint" {
  description = "Endpoint for the metadata database"
  value       = module.metadata_store.db_endpoint
  sensitive   = true
}

output "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  value       = module.compute.cluster_name
}

output "monitoring_dashboard_url" {
  description = "URL for the monitoring dashboard"
  value       = module.monitoring.dashboard_url
}
