output "endpoint" {
  description = "Message bus endpoint"
  value = var.message_bus_type == "redis" ? (
    length(aws_elasticache_cluster.redis) > 0 ? 
    aws_elasticache_cluster.redis[0].cache_nodes[0].address : ""
  ) : (
    length(aws_msk_cluster.kafka) > 0 ?
    aws_msk_cluster.kafka[0].bootstrap_brokers : ""
  )
}
