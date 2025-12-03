variable "environment" {
  description = "Environment name"
  type        = string
}

variable "message_bus_type" {
  description = "Type of message bus (kafka or redis)"
  type        = string
}

variable "vpc_id" {
  description = "VPC ID"
  type        = string
}

variable "private_subnet_ids" {
  description = "Private subnet IDs"
  type        = list(string)
}
