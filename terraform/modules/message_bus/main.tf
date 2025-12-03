resource "aws_security_group" "message_bus" {
  name_prefix = "${var.environment}-message-bus-"
  vpc_id      = var.vpc_id
  
  ingress {
    from_port   = var.message_bus_type == "redis" ? 6379 : 9092
    to_port     = var.message_bus_type == "redis" ? 6379 : 9092
    protocol    = "tcp"
    cidr_blocks = ["10.0.0.0/16"]
  }
  
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }
  
  tags = {
    Name        = "${var.environment}-message-bus-sg"
    Environment = var.environment
  }
}

# Redis ElastiCache
resource "aws_elasticache_subnet_group" "redis" {
  count      = var.message_bus_type == "redis" ? 1 : 0
  name       = "${var.environment}-redis-subnet-group"
  subnet_ids = var.private_subnet_ids
}

resource "aws_elasticache_cluster" "redis" {
  count                = var.message_bus_type == "redis" ? 1 : 0
  cluster_id           = "${var.environment}-etl-redis"
  engine               = "redis"
  node_type            = "cache.t3.medium"
  num_cache_nodes      = 1
  parameter_group_name = "default.redis7"
  engine_version       = "7.0"
  port                 = 6379
  subnet_group_name    = aws_elasticache_subnet_group.redis[0].name
  security_group_ids   = [aws_security_group.message_bus.id]
  
  tags = {
    Name        = "${var.environment}-etl-redis"
    Environment = var.environment
  }
}

# MSK (Kafka) - Placeholder for Kafka implementation
resource "aws_msk_cluster" "kafka" {
  count              = var.message_bus_type == "kafka" ? 1 : 0
  cluster_name       = "${var.environment}-etl-kafka"
  kafka_version      = "3.5.1"
  number_of_broker_nodes = 2
  
  broker_node_group_info {
    instance_type   = "kafka.t3.small"
    client_subnets  = var.private_subnet_ids
    security_groups = [aws_security_group.message_bus.id]
    
    storage_info {
      ebs_storage_info {
        volume_size = 100
      }
    }
  }
  
  tags = {
    Name        = "${var.environment}-etl-kafka"
    Environment = var.environment
  }
}
