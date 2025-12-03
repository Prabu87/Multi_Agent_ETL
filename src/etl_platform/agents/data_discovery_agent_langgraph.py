"""Data Discovery Agent using LangChain and LangGraph architecture."""

from typing import Any, Dict, List, Literal, Optional
from datetime import datetime
import logging

from langchain_core.messages import HumanMessage, AIMessage
from langgraph.graph import StateGraph, END

from etl_platform.agents.base_agent import BaseAgent, AgentState
from etl_platform.shared.models import (
    ConnectionConfig,
    DataSource,
    DataSourceType,
    Field,
    Schema,
    SchemaChange,
    SourceMetadata,
)
from etl_platform.shared.message_bus import MessageBus


logger = logging.getLogger(__name__)


class DiscoveryState(AgentState):
    """Extended state for data discovery operations."""
    connection_config: Optional[ConnectionConfig]
    discovered_sources: List[DataSource]
    current_source: Optional[DataSource]
    extracted_metadata: Optional[SourceMetadata]
    schema_changes: List[SchemaChange]
    schema_cache: Dict[str, Schema]


class DataDiscoveryAgentLangGraph(BaseAgent):
    """
    Data Discovery Agent using LangGraph for orchestrating discovery workflow.
    
    This agent uses a graph-based architecture to:
    1. Discover data sources from configured connections
    2. Extract metadata and schemas
    3. Detect schema changes
    4. Update catalog and publish events
    """
    
    def __init__(self, message_bus: MessageBus, agent_id: Optional[str] = None):
        """
        Initialize the Data Discovery Agent.
        
        Args:
            message_bus: Message bus for publishing discovery events
            agent_id: Unique identifier for this agent instance
        """
        self._schema_cache: Dict[str, Schema] = {}
        super().__init__(message_bus, agent_id, agent_type="data-discovery")
    
    def _build_graph(self) -> StateGraph:
        """
        Build the discovery workflow graph.
        
        Graph structure:
        START -> discover_sources -> extract_metadata -> detect_changes -> 
        update_catalog -> publish_events -> END
        """
        workflow = StateGraph(DiscoveryState)
        
        # Add nodes for each step in the discovery process
        workflow.add_node("discover_sources", self._discover_sources_node)
        workflow.add_node("extract_metadata", self._extract_metadata_node)
        workflow.add_node("detect_changes", self._detect_changes_node)
        workflow.add_node("update_catalog", self._update_catalog_node)
        workflow.add_node("publish_events", self._publish_events_node)
        
        # Define the workflow edges
        workflow.set_entry_point("discover_sources")
        workflow.add_edge("discover_sources", "extract_metadata")
        workflow.add_edge("extract_metadata", "detect_changes")
        workflow.add_edge("detect_changes", "update_catalog")
        workflow.add_edge("update_catalog", "publish_events")
        workflow.add_edge("publish_events", END)
        
        return workflow.compile()
    
    def _discover_sources_node(self, state: DiscoveryState) -> DiscoveryState:
        """
        Node: Discover data sources from connection configuration.
        
        Args:
            state: Current agent state
            
        Returns:
            Updated state with discovered sources
        """
        logger.info("Node: discover_sources")
        
        connection_config = state.get("connection_config")
        if not connection_config:
            state["error"] = "No connection configuration provided"
            return state
        
        try:
            sources = self._discover_sources(connection_config)
            state["discovered_sources"] = sources
            state["messages"].append(
                AIMessage(content=f"Discovered {len(sources)} data sources")
            )
            logger.info(f"Discovered {len(sources)} sources")
        except Exception as e:
            state["error"] = f"Discovery failed: {str(e)}"
            logger.error(f"Discovery failed: {str(e)}")
        
        return state
    
    def _extract_metadata_node(self, state: DiscoveryState) -> DiscoveryState:
        """
        Node: Extract metadata from discovered sources.
        
        Args:
            state: Current agent state
            
        Returns:
            Updated state with extracted metadata
        """
        logger.info("Node: extract_metadata")
        
        sources = state.get("discovered_sources", [])
        if not sources:
            state["error"] = "No sources to extract metadata from"
            return state
        
        # For now, process the first source (can be extended to process all)
        source = sources[0]
        state["current_source"] = source
        
        try:
            metadata = self._extract_metadata(source)
            state["extracted_metadata"] = metadata
            state["messages"].append(
                AIMessage(content=f"Extracted metadata for {source.name}")
            )
            logger.info(f"Extracted metadata for {source.id}")
        except Exception as e:
            state["error"] = f"Metadata extraction failed: {str(e)}"
            logger.error(f"Metadata extraction failed: {str(e)}")
        
        return state
    
    def _detect_changes_node(self, state: DiscoveryState) -> DiscoveryState:
        """
        Node: Detect schema changes by comparing with cached schemas.
        
        Args:
            state: Current agent state
            
        Returns:
            Updated state with detected changes
        """
        logger.info("Node: detect_changes")
        
        metadata = state.get("extracted_metadata")
        if not metadata:
            return state
        
        # Use instance schema cache
        state["schema_cache"] = self._schema_cache
        
        try:
            changes = self._detect_schema_changes(
                metadata.source_id,
                metadata.schema
            )
            state["schema_changes"] = changes
            state["messages"].append(
                AIMessage(content=f"Detected {len(changes)} schema changes")
            )
            logger.info(f"Detected {len(changes)} schema changes")
        except Exception as e:
            state["error"] = f"Change detection failed: {str(e)}"
            logger.error(f"Change detection failed: {str(e)}")
        
        return state
    
    def _update_catalog_node(self, state: DiscoveryState) -> DiscoveryState:
        """
        Node: Update the data catalog with extracted metadata.
        
        Args:
            state: Current agent state
            
        Returns:
            Updated state
        """
        logger.info("Node: update_catalog")
        
        metadata = state.get("extracted_metadata")
        if not metadata:
            return state
        
        # In a real implementation, this would persist to a database
        # For now, we just prepare the data for publishing
        state["messages"].append(
            AIMessage(content=f"Updated catalog for {metadata.source_id}")
        )
        logger.info(f"Catalog updated for {metadata.source_id}")
        
        return state
    
    def _publish_events_node(self, state: DiscoveryState) -> DiscoveryState:
        """
        Node: Publish discovery and schema change events to message bus.
        
        Args:
            state: Current agent state
            
        Returns:
            Updated state with result
        """
        logger.info("Node: publish_events")
        
        metadata = state.get("extracted_metadata")
        changes = state.get("schema_changes", [])
        
        if not metadata:
            return state
        
        # Publish discovery event
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
        
        self.publish_event(
            event_type="data.discovery.completed",
            payload=event_payload,
            topic="discovery.events"
        )
        
        # Publish schema change event if changes detected
        if changes:
            self.publish_event(
                event_type="schema.changed",
                payload={
                    "source_id": metadata.source_id,
                    "changes": event_payload["schema_changes"]
                },
                topic="schema.events"
            )
        
        state["result"] = event_payload
        state["messages"].append(
            AIMessage(content="Published discovery events")
        )
        
        return state
    
    # Core discovery methods (same as before but extracted for reuse)
    
    def _discover_sources(self, connection_config: ConnectionConfig) -> List[DataSource]:
        """Discover data sources from connection configuration."""
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
        
        return sources
    
    def _extract_metadata(self, source: DataSource) -> SourceMetadata:
        """Extract metadata from a data source."""
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
        
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
        
        with engine.connect() as conn:
            result = conn.execute(text(
                f"SELECT pg_total_relation_size('{table_name}') as size"
            ))
            size_bytes = result.scalar()
        
        engine.dispose()
        
        return SourceMetadata(
            source_id=source.id,
            schema=schema,
            row_count=row_count,
            size_bytes=size_bytes,
            last_modified=datetime.now(),
            statistics={"column_count": len(fields)}
        )
    
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
        
        with engine.connect() as conn:
            result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            row_count = result.scalar()
        
        engine.dispose()
        
        return SourceMetadata(
            source_id=source.id,
            schema=schema,
            row_count=row_count,
            last_modified=datetime.now(),
            statistics={"column_count": len(fields)}
        )
    
    def _extract_s3_metadata(self, source: DataSource) -> SourceMetadata:
        """Extract metadata from an S3 object."""
        import boto3
        
        config = source.connection_config
        s3_client = boto3.client('s3', region_name=config.region)
        
        bucket = source.metadata["bucket"]
        key = source.metadata["key"]
        
        response = s3_client.head_object(Bucket=bucket, Key=key)
        
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
        
        return SourceMetadata(
            source_id=source.id,
            schema=schema,
            size_bytes=response.get('ContentLength'),
            last_modified=response.get('LastModified'),
            statistics={
                "content_type": response.get('ContentType'),
                "etag": response.get('ETag')
            }
        )
    
    def _detect_schema_changes(self, source_id: str, new_schema: Schema) -> List[SchemaChange]:
        """Detect changes between cached schema and new schema."""
        if source_id not in self._schema_cache:
            self._schema_cache[source_id] = new_schema
            return []
        
        old_schema = self._schema_cache[source_id]
        changes = []
        
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
        
        # Detect modified fields
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
        
        if changes:
            self._schema_cache[source_id] = new_schema
        
        return changes
    
    def discover_and_catalog(self, connection_config: ConnectionConfig) -> Dict[str, Any]:
        """
        High-level method to discover sources and catalog them.
        
        Args:
            connection_config: Configuration for connecting to data sources
            
        Returns:
            Result dictionary with discovery information
        """
        initial_state: DiscoveryState = {
            "messages": [HumanMessage(content="Start data discovery")],
            "task_id": f"discovery-{datetime.now().timestamp()}",
            "context": {},
            "result": None,
            "error": None,
            "connection_config": connection_config,
            "discovered_sources": [],
            "current_source": None,
            "extracted_metadata": None,
            "schema_changes": [],
            "schema_cache": self._schema_cache
        }
        
        final_state = self.execute(initial_state)
        
        if final_state.get("error"):
            raise Exception(final_state["error"])
        
        return final_state.get("result", {})
