variable "aws_region" {
  description = "AWS region for infrastructure deployment"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "vpc_cidr" {
  description = "CIDR block for VPC"
  type        = string
  default     = "10.0.0.0/16"
}

variable "message_bus_type" {
  description = "Type of message bus (kafka or redis)"
  type        = string
  default     = "redis"
  
  validation {
    condition     = contains(["kafka", "redis"], var.message_bus_type)
    error_message = "Message bus type must be either 'kafka' or 'redis'."
  }
}

variable "db_instance_class" {
  description = "RDS instance class for metadata database"
  type        = string
  default     = "db.t3.medium"
}

variable "db_name" {
  description = "Name of the metadata database"
  type        = string
  default     = "etl_metadata"
}
