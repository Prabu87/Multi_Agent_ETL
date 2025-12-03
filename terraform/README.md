# Terraform Infrastructure

This directory contains Terraform configurations for provisioning the Multi-Agent ETL Platform infrastructure on AWS.

## Architecture

The infrastructure includes:

- **Networking**: VPC with public and private subnets across multiple availability zones
- **Message Bus**: Redis (ElastiCache) or Kafka (MSK) for inter-component communication
- **Metadata Store**: PostgreSQL (RDS) for storing catalog, schemas, and agent decisions
- **Compute**: ECS Fargate cluster for running agents and pipeline engines
- **Monitoring**: CloudWatch logs, dashboards, and alarms

## Prerequisites

- Terraform >= 1.0
- AWS CLI configured with appropriate credentials
- AWS account with necessary permissions

## Usage

### Initialize Terraform

```bash
cd terraform
terraform init
```

### Plan Infrastructure

```bash
terraform plan -var="environment=dev"
```

### Apply Infrastructure

```bash
terraform apply -var="environment=dev"
```

### Destroy Infrastructure

```bash
terraform destroy -var="environment=dev"
```

## Configuration

### Variables

Key variables can be configured in `terraform.tfvars`:

```hcl
aws_region       = "us-east-1"
environment      = "dev"
vpc_cidr         = "10.0.0.0/16"
message_bus_type = "redis"  # or "kafka"
db_instance_class = "db.t3.medium"
```

### Outputs

After applying, Terraform will output:

- VPC ID
- Message bus endpoint
- Metadata database endpoint
- ECS cluster name
- Monitoring dashboard URL

## Modules

### networking

Creates VPC, subnets, NAT gateways, and routing tables.

### message_bus

Provisions either Redis (ElastiCache) or Kafka (MSK) based on configuration.

### metadata_store

Creates PostgreSQL RDS instance with automated backups and security groups.

### compute

Sets up ECS Fargate cluster with IAM roles and security groups.

### monitoring

Configures CloudWatch logs, dashboards, and alarms.

## Security

- Database passwords are stored in AWS Secrets Manager
- All resources are deployed in private subnets where possible
- Security groups restrict access to necessary ports only
- IAM roles follow principle of least privilege

## Cost Optimization

For development environments:
- Use `db.t3.small` for RDS
- Use `cache.t3.micro` for Redis
- Enable FARGATE_SPOT for ECS tasks
- Reduce backup retention periods
