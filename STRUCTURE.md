# Project Structure

This document describes the directory structure and organization of the Multi-Agent ETL Platform.

## Directory Layout

```
multi-agent-etl-platform/
├── .kiro/                          # Kiro specifications
│   └── specs/
│       └── multi-agent-etl-platform/
│           ├── design.md           # Design document
│           ├── requirements.md     # Requirements document
│           └── tasks.md            # Implementation tasks
│
├── src/                            # Source code
│   └── etl_platform/
│       ├── __init__.py
│       ├── agents/                 # AI Agents
│       │   └── __init__.py
│       ├── engines/                # Pipeline Engines
│       │   └── __init__.py
│       ├── services/               # Core Services
│       │   └── __init__.py
│       └── shared/                 # Shared Utilities
│           ├── __init__.py
│           └── message_bus.py      # Message bus implementations
│
├── tests/                          # Test suite
│   ├── __init__.py
│   ├── conftest.py                 # Pytest configuration
│   └── test_message_bus.py         # Message bus tests
│
├── terraform/                      # Infrastructure as Code
│   ├── main.tf                     # Main Terraform configuration
│   ├── variables.tf                # Input variables
│   ├── outputs.tf                  # Output values
│   ├── README.md                   # Terraform documentation
│   └── modules/                    # Terraform modules
│       ├── networking/             # VPC and networking
│       ├── message_bus/            # Redis/Kafka setup
│       ├── metadata_store/         # PostgreSQL RDS
│       ├── compute/                # ECS Fargate
│       └── monitoring/             # CloudWatch
│
├── .gitignore                      # Git ignore rules
├── pyproject.toml                  # Python project configuration
├── requirements.txt                # Core dependencies
├── requirements-dev.txt            # Development dependencies
├── setup.py                        # Setup script
├── README.md                       # Project documentation
└── STRUCTURE.md                    # This file

## Component Organization

### Agents (`src/etl_platform/agents/`)

Will contain:
- `data_discovery.py` - Data Discovery Agent
- `schema_mapping.py` - Schema Mapping Agent
- `error_resolution.py` - Error Resolution Agent

### Engines (`src/etl_platform/engines/`)

Will contain:
- `batch_pipeline.py` - Batch Pipeline Engine
- `stream_pipeline.py` - Stream Pipeline Engine

### Services (`src/etl_platform/services/`)

Will contain:
- `orchestrator.py` - Orchestrator Service
- `metadata_store.py` - Metadata Store Service

### Shared (`src/etl_platform/shared/`)

Contains:
- `message_bus.py` - Message bus implementations (InMemory, Redis, Kafka)

Will contain:
- `models.py` - Data models and schemas
- `config.py` - Configuration management
- `utils.py` - Utility functions

## Testing Structure

Tests are organized to mirror the source code structure:

```
tests/
├── test_agents/
├── test_engines/
├── test_services/
└── test_shared/
    └── test_message_bus.py
```

## Infrastructure Modules

Each Terraform module is self-contained with:
- `main.tf` - Resource definitions
- `variables.tf` - Input variables
- `outputs.tf` - Output values

## Development Workflow

1. **Setup**: Run `python setup.py` to install dependencies
2. **Development**: Implement features in `src/etl_platform/`
3. **Testing**: Write tests in `tests/` and run with `pytest`
4. **Infrastructure**: Configure and deploy with Terraform

## Configuration Files

- `pyproject.toml` - Python project metadata, dependencies, and tool configuration
- `requirements.txt` - Core runtime dependencies
- `requirements-dev.txt` - Development and testing dependencies
- `.gitignore` - Files and directories to exclude from version control
