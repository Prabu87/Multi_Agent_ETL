# Multi-Agent ETL Platform

A distributed ETL platform that combines traditional ETL capabilities with autonomous AI agents to create self-managing data pipelines.

## Features

- **Batch Processing**: Schedule and execute large-volume data processing jobs
- **Event Streaming**: Real-time data processing with low latency
- **AI Agents**: Autonomous agents for data discovery, schema mapping, and error resolution
- **Self-Healing**: Automatic error detection and resolution
- **Schema Evolution**: Automatic handling of schema changes

## Architecture

The platform uses a microservices architecture with:
- Data Discovery Agent
- Schema Mapping Agent
- Error Resolution Agent
- Batch Pipeline Engine
- Stream Pipeline Engine
- Orchestrator Service
- Metadata Store

## Setup

### Prerequisites

- Python 3.10+
- Poetry
- Terraform (for infrastructure provisioning)

### Installation

1. Install dependencies:
```bash
poetry install
```

2. Activate virtual environment:
```bash
poetry shell
```

3. Run tests:
```bash
pytest
```

## Infrastructure

Infrastructure is managed using Terraform. See `terraform/` directory for configuration.

## Development

Run tests with coverage:
```bash
pytest --cov=etl_platform tests/
```

Run property-based tests:
```bash
pytest -v tests/ -k property
```
