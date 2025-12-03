"""Data Discovery Agent for automatic data source discovery and cataloging."""

import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any
import logging

from etl_platform.shared.models import (
    ConnectionConfig,
    DataSource,
    DataSourceType,
    Field,
    Schema,
    SchemaChange,
    SourceMetadata,
)
from etl_platform.shared.message_bus import Message, MessageBus


logger = logging.getLogger(__name__)


class DataDiscoveryAgent:
    """Agent responsible for discovering data sources and extracting metadata."""
    
    def __init__(self, message_bus: MessageBus, agent_id: Optional[str] = None):
        """
        Initialize the Data Discovery Agent.
        
        Args:
            message_bus: Message bus for publishing discovery events
            agent_id: Unique identifier for this agent instance
        """
        self.message_bus = message_bus
        self.agent_id = agent_id or f"data-discovery-{uuid.uuid4().hex[:8]}"
        self._schema_cache: Dict[str, Schema] = {}
        logger.info(f"Data Discovery Agent initialized: {self.agent_id}")
    
    def discover_sources(self, connection_config: ConnectionConfig) -> List[DataSource]:
        """
        Discover data sources from the given connection configuration.
        
        Args:
            connection_config: Configuration for connecting to data sources
            
        Returns:
            List of discovered data sources
        """
        logger.info(f"Discovering sources for type: {connection_config.source_type}")
        
        if connection_config.source_type == DataSourceType.POSTGRESQL:
            return self._discover_postgresql_sources(connection_config)
        elif connection_config.source_type == DataSourceType.MYSQL:
            return self._discover_mysql_sources(connection_config)
        elif connection_config.source_type == DataSourceType.S3:
            return self._discover_s3_sources(connection_config)
        else:
            raise ValueError(f"Unsupported source type: {connection_config.source_type}")
    
    def _discover_postgresql_sources(
        self, connection_config: ConnectionConfig
    ) -> List[DataSource]:
        """Discover PostgreSQL tables as data sources."""
        from sqlalchemy import create_engine, inspect
        
        connection_string = (
            f"postgresql://{connection_config.username}:{connection_config.password}"
            f"@{connection_config.host}:{connection_config.port}/{connection_config.database}"
        )
        
        engine = create_engine(connection_string)
        inspector = inspect(engine)
        
        sources = []
        for table_name in inspector.get_table_names():
            source_id = f"pg_{connection_config.database}_{table_name}"
            source = DataSource(
                id=source_id,
                name=table_name,
                source_type=DataSourceType.POSTGRESQL,
                connection_config=connection_config,
                discovered_at=datetime.now(),
                metadata={"database": connection_config.database, "table": table_name}
            )
            sources.append(source)
        
        engine.dispose()
        logger.info(f"Discovered {len(sources)} PostgreSQL tables")
        return sources
    
    def _discover_mysql_sources(
        self, connection_config: ConnectionConfig
    ) -> List[DataSource]:
        """Discover MySQL tables as data sources."""
        from sqlalchemy import create_engine, inspect
        
        connection_string = (
            f"mysql+pymysql://{connection_config.username}:{connection_config.password}"
            f"@{connection_config.host}:{connection_config.port}/{connection_config.database}"
        )
        
        engine = create_engine(connection_string)
        inspector = inspect(engine)
        
        sources = []
        for table_name in inspector.get_table_names():
            source_id = f"mysql_{connection_config.database}_{table_name}"
            source = DataSource(
                id=source_id,
                name=table_name,
                source_type=DataSourceType.MYSQL,
                connection_config=connection_config,
                discovered_at=datetime.now(),
                metadata={"database": connection_config.database, "table": table_name}
            )
            sources.append(source)
        
        engine.dispose()
        logger.info(f"Discovered {len(sources)} MySQL tables")
        return sources
    
    def _discover_s3_sources(
        self, connection_config: ConnectionConfig
    ) -> List[DataSource]:
        """Discover S3 objects as data sources."""
        import boto3
        
        s3_client = boto3.client('s3', region_name=connection_config.region)
        
        sources = []
        response = s3_client.list_objects_v2(Bucket=connection_config.bucket)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                source_id = f"s3_{connection_config.bucket}_{key.replace('/', '_')}"
                source = DataSource(
                    id=source_id,
                    name=key,
                    source_type=DataSourceType.S3,
                    connection_config=connection_config,
                    discovered_at=datetime.now(),
                    metadata={
                        "bucket": connection_config.bucket,
                        "key": key,
                        "size": obj.get('Size'),
                        "last_modified": obj.get('LastModified')
                    }
                )
                sources.append(source)
        
        logger.info(f"Discovered {len(sources)} S3 objects")
        return sources

    
    def extract_metadata(self, source: DataSource) -> SourceMetadata:
        """
        Extract metadata from a data source.
        
        Args:
            source: Data source to extract metadata from
            
        Returns:
            Extracted source metadata including schema and statistics
        """
        logger.info(f"Extracting metadata for source: {source.id}")
        
        if source.source_type == DataSourceType.POSTGRESQL:
            return self._extract_postgresql_metadata(source)
        elif source.source_type == DataSourceType.MYSQL:
            return self._extract_mysql_metadata(source)
        elif source.source_type == DataSourceType.S3:
            return self._extract_s3_metadata(source)
        else:
            raise ValueError(f"Unsupported source type: {source.source_type}")
    
    def _extract_postgresql_metadata(self, source: DataSource) -> SourceMetadata:
        """Extract metadata from a PostgreSQL table."""
        from sqlalchemy import create_engine, inspect, text
        
        config = source.connection_config
        connection_string = (
            f"postgresql://{config.username}:{config.password}"
            f"@{config.host}:{config.port}/{config.database}"
        )
        
        engine = create_engine(connection_string)
        inspector = inspect(engine)
        
        table_name = source.metadata["table"]
        columns = inspector.get_columns(table_name)
        
        # Extract schema
        fields = []
        for col in columns:
            field = Field(
                name=col["name"],
                data_type=str(col["type"]),
                nullable=col["nullable"],
                description=col.get("comment")
            )
            fields.append(field)
        
        schema = Schema(
            id=f"{source.id}_schema_v1",
            source_id=source.id,
            version=1,
            fields=fields,
            timestamp=datetime.now(),
            table_name=table_name
        )
        
        # Get row count
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
        
        # Get table size
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT pg_total_relation_size('{table_name}') as size"
            ))
            size_bytes = result.scalar()
        
        engine.dispose()
        
        metadata = SourceMetadata(
            source_id=source.id,
            schema=schema,
            row_count=row_count,
            size_bytes=size_bytes,
            last_modified=datetime.now(),
            statistics={"column_count": len(fields)}
        )
        
        logger.info(f"Extracted metadata: {row_count} rows, {len(fields)} columns")
        return metadata
    
    def _extract_mysql_metadata(self, source: DataSource) -> SourceMetadata:
        """Extract metadata from a MySQL table."""
        from sqlalchemy import create_engine, inspect, text
        
        config = source.connection_config
        connection_string = (
            f"mysql+pymysql://{config.username}:{config.password}"
            f"@{config.host}:{config.port}/{config.database}"
        )
        
        engine = create_engine(connection_string)
        inspector = inspect(engine)
        
        table_name = source.metadata["table"]
        columns = inspector.get_columns(table_name)
        
        # Extract schema
        fields = []
        for col in columns:
            field = Field(
                name=col["name"],
                data_type=str(col["type"]),
                nullable=col["nullable"],
                description=col.get("comment")
            )
            fields.append(field)
        
        schema = Schema(
            id=f"{source.id}_schema_v1",
            source_id=source.id,
            version=1,
            fields=fields,
            timestamp=datetime.now(),
            table_name=table_name
        )
        
        # Get row count
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
        
        engine.dispose()
        
        metadata = SourceMetadata(
            source_id=source.id,
            schema=schema,
            row_count=row_count,
            last_modified=datetime.now(),
            statistics={"column_count": len(fields)}
        )
        
        logger.info(f"Extracted metadata: {row_count} rows, {len(fields)} columns")
        return metadata
    
    def _extract_s3_metadata(self, source: DataSource) -> SourceMetadata:
        """Extract metadata from an S3 object."""
        import boto3
        
        config = source.connection_config
        s3_client = boto3.client('s3', region_name=config.region)
        
        bucket = source.metadata["bucket"]
        key = source.metadata["key"]
        
        # Get object metadata
        response = s3_client.head_object(Bucket=bucket, Key=key)
        
        # For S3, we create a simple schema based on the file type
        # In a real implementation, this would parse the file content
        fields = [
            Field(name="content", data_type="text", nullable=False)
        ]
        
        schema = Schema(
            id=f"{source.id}_schema_v1",
            source_id=source.id,
            version=1,
            fields=fields,
            timestamp=datetime.now()
        )
        
        metadata = SourceMetadata(
            source_id=source.id,
            schema=schema,
            size_bytes=response.get('ContentLength'),
            last_modified=response.get('LastModified'),
            statistics={
                "content_type": response.get('ContentType'),
                "etag": response.get('ETag')
            }
        )
        
        logger.info(f"Extracted S3 metadata: {metadata.size_bytes} bytes")
        return metadata
    
    def detect_schema_changes(self, source_id: str, new_schema: Schema) -> List[SchemaChange]:
        """
        Detect changes between cached schema and new schema.
        
        Args:
            source_id: ID of the data source
            new_schema: Newly extracted schema
            
        Returns:
            List of detected schema changes
        """
        if source_id not in self._schema_cache:
            logger.info(f"No cached schema for {source_id}, storing new schema")
            self._schema_cache[source_id] = new_schema
            return []
        
        old_schema = self._schema_cache[source_id]
        changes = []
        
        # Create field maps for comparison
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        
        # Detect added fields
        for field_name in new_fields:
            if field_name not in old_fields:
                changes.append(SchemaChange(
                    source_id=source_id,
                    change_type="added",
                    field_name=field_name,
                    new_value=new_fields[field_name].data_type
                ))
        
        # Detect removed fields
        for field_name in old_fields:
            if field_name not in new_fields:
                changes.append(SchemaChange(
                    source_id=source_id,
                    change_type="removed",
                    field_name=field_name,
                    old_value=old_fields[field_name].data_type
                ))
        
        # Detect modified fields (type changes)
        for field_name in old_fields:
            if field_name in new_fields:
                old_field = old_fields[field_name]
                new_field = new_fields[field_name]
                
                if old_field.data_type != new_field.data_type:
                    changes.append(SchemaChange(
                        source_id=source_id,
                        change_type="type_changed",
                        field_name=field_name,
                        old_value=old_field.data_type,
                        new_value=new_field.data_type
                    ))
                
                if old_field.nullable != new_field.nullable:
                    changes.append(SchemaChange(
                        source_id=source_id,
                        change_type="modified",
                        field_name=field_name,
                        old_value=f"nullable={old_field.nullable}",
                        new_value=f"nullable={new_field.nullable}"
                    ))
        
        # Update cache with new schema
        if changes:
            logger.info(f"Detected {len(changes)} schema changes for {source_id}")
            self._schema_cache[source_id] = new_schema
        
        return changes
    
    def update_catalog(self, metadata: SourceMetadata) -> None:
        """
        Update the data catalog with extracted metadata and publish discovery event.
        
        Args:
            metadata: Source metadata to add to catalog
        """
        logger.info(f"Updating catalog for source: {metadata.source_id}")
        
        # Detect schema changes
        changes = self.detect_schema_changes(metadata.source_id, metadata.schema)
        
        # Publish discovery event to message bus
        event_payload = {
            "source_id": metadata.source_id,
            "schema_version": metadata.schema.version,
            "row_count": metadata.row_count,
            "size_bytes": metadata.size_bytes,
            "field_count": len(metadata.schema.fields),
            "schema_changes": [
                {
                    "change_type": change.change_type,
                    "field_name": change.field_name,
                    "old_value": change.old_value,
                    "new_value": change.new_value
                }
                for change in changes
            ]
        }
        
        message = Message(
            event_type="data.discovery.completed",
            payload=event_payload,
            timestamp=datetime.now(),
            source=self.agent_id
        )
        
        self.message_bus.publish("discovery.events", message)
        logger.info(f"Published discovery event for {metadata.source_id}")
        
        # If schema changes detected, publish schema change event
        if changes:
            schema_change_message = Message(
                event_type="schema.changed",
                payload={
                    "source_id": metadata.source_id,
                    "changes": event_payload["schema_changes"]
                },
                timestamp=datetime.now(),
                source=self.agent_id
            )
            self.message_bus.publish("schema.events", message)
            logger.info(f"Published schema change event for {metadata.source_id}")
