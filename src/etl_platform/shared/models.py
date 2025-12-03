"""Core data models for the ETL platform."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional
from enum import Enum


class DataSourceType(str, Enum):
    """Supported data source types."""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    S3 = "s3"


@dataclass
class ConnectionConfig:
    """Configuration for connecting to a data source."""
    source_type: DataSourceType
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    bucket: Optional[str] = None  # For S3
    region: Optional[str] = None  # For S3
    extra_params: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Field:
    """Represents a field in a schema."""
    name: str
    data_type: str
    nullable: bool
    description: Optional[str] = None


@dataclass
class Schema:
    """Represents a data schema."""
    id: str
    source_id: str
    version: int
    fields: List[Field]
    timestamp: datetime
    table_name: Optional[str] = None


@dataclass
class DataSource:
    """Represents a discovered data source."""
    id: str
    name: str
    source_type: DataSourceType
    connection_config: ConnectionConfig
    discovered_at: datetime
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SourceMetadata:
    """Metadata extracted from a data source."""
    source_id: str
    schema: Schema
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    last_modified: Optional[datetime] = None
    statistics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaChange:
    """Represents a change detected in a schema."""
    source_id: str
    change_type: str  # 'added', 'removed', 'modified', 'type_changed'
    field_name: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    detected_at: datetime = field(default_factory=datetime.now)


@dataclass
class CatalogEntry:
    """Entry in the data catalog."""
    source_id: str
    name: str
    source_type: DataSourceType
    schema: Schema
    metadata: SourceMetadata
    created_at: datetime
    updated_at: datetime


class MappingType(str, Enum):
    """Types of field mappings."""
    DIRECT = "direct"
    TRANSFORMED = "transformed"
    DERIVED = "derived"


@dataclass
class FieldMapping:
    """Represents a mapping between source and target fields."""
    source_field: str
    target_field: str
    transformation: Optional[str] = None
    confidence: float = 0.0  # 0.0 to 1.0
    mapping_type: MappingType = MappingType.DIRECT


@dataclass
class TransformationLogic:
    """Represents transformation logic for a field mapping."""
    mapping: FieldMapping
    sql_logic: Optional[str] = None
    python_logic: Optional[str] = None
    description: str = ""
