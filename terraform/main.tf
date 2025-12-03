terraform {
  required_version = ">= 1.0"
  
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

# VPC and Networking
module "networking" {
  source = "./modules/networking"
  
  environment = var.environment
  vpc_cidr    = var.vpc_cidr
}

# Message Bus (Kafka or Redis)
module "message_bus" {
  source = "./modules/message_bus"
  
  environment       = var.environment
  message_bus_type  = var.message_bus_type
  vpc_id            = module.networking.vpc_id
  private_subnet_ids = module.networking.private_subnet_ids
}

# Metadata Database (PostgreSQL)
module "metadata_store" {
  source = "./modules/metadata_store"
  
  environment        = var.environment
  vpc_id             = module.networking.vpc_id
  private_subnet_ids = module.networking.private_subnet_ids
  db_instance_class  = var.db_instance_class
  db_name            = var.db_name
}

# Compute Resources (ECS)
module "compute" {
  source = "./modules/compute"
  
  environment        = var.environment
  vpc_id             = module.networking.vpc_id
  private_subnet_ids = module.networking.private_subnet_ids
  public_subnet_ids  = module.networking.public_subnet_ids
}

# Monitoring Infrastructure
module "monitoring" {
  source = "./modules/monitoring"
  
  environment = var.environment
}
