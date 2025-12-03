"""Shared utilities and common code."""

from etl_platform.shared.message_bus import Message, MessageBus, InMemoryMessageBus
from etl_platform.shared.models import (
    ConnectionConfig,
    DataSource,
    DataSourceType,
    Field,
    Schema,
    SchemaChange,
    SourceMetadata,
    CatalogEntry,
    FieldMapping,
    MappingType,
    TransformationLogic,
)

__all__ = [
    "Message",
    "MessageBus",
    "InMemoryMessageBus",
    "ConnectionConfig",
    "DataSource",
    "DataSourceType",
    "Field",
    "Schema",
    "SchemaChange",
    "SourceMetadata",
    "CatalogEntry",
    "FieldMapping",
    "MappingType",
    "TransformationLogic",
]
