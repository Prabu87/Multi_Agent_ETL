"""Tests for Data Discovery Agent."""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from etl_platform.agents import DataDiscoveryAgent
from etl_platform.shared import (
    ConnectionConfig,
    DataSource,
    DataSourceType,
    Field,
    Schema,
    SchemaChange,
    SourceMetadata,
    InMemoryMessageBus,
)


@pytest.fixture
def message_bus():
    """Provide an in-memory message bus for testing."""
    bus = InMemoryMessageBus()
    yield bus
    bus.close()


@pytest.fixture
def agent(message_bus):
    """Provide a Data Discovery Agent instance."""
    return DataDiscoveryAgent(message_bus=message_bus, agent_id="test-agent")


@pytest.fixture
def postgres_config():
    """Provide a PostgreSQL connection configuration."""
    return ConnectionConfig(
        source_type=DataSourceType.POSTGRESQL,
        host="localhost",
        port=5432,
        database="testdb",
        username="testuser",
        password="testpass"
    )


@pytest.fixture
def mysql_config():
    """Provide a MySQL connection configuration."""
    return ConnectionConfig(
        source_type=DataSourceType.MYSQL,
        host="localhost",
        port=3306,
        database="testdb",
        username="testuser",
        password="testpass"
    )


@pytest.fixture
def s3_config():
    """Provide an S3 connection configuration."""
    return ConnectionConfig(
        source_type=DataSourceType.S3,
        bucket="test-bucket",
        region="us-east-1"
    )


class TestDataDiscoveryAgent:
    """Test suite for Data Discovery Agent."""
    
    def test_agent_initialization(self, message_bus):
        """Test agent initializes with correct attributes."""
        agent = DataDiscoveryAgent(message_bus=message_bus, agent_id="custom-agent")
        assert agent.agent_id == "custom-agent"
        assert agent.message_bus == message_bus
        assert isinstance(agent._schema_cache, dict)
    
    def test_agent_auto_generates_id(self, message_bus):
        """Test agent auto-generates ID if not provided."""
        agent = DataDiscoveryAgent(message_bus=message_bus)
        assert agent.agent_id.startswith("data-discovery-")
    
    @patch('sqlalchemy.create_engine')
    @patch('sqlalchemy.inspect')
    def test_discover_postgresql_sources(self, mock_inspect, mock_create_engine, agent, postgres_config):
        """Test discovering PostgreSQL tables."""
        # Mock SQLAlchemy engine and inspector
        mock_engine = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["users", "orders", "products"]
        
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        sources = agent.discover_sources(postgres_config)
        
        assert len(sources) == 3
        assert all(isinstance(s, DataSource) for s in sources)
        assert sources[0].name == "users"
        assert sources[1].name == "orders"
        assert sources[2].name == "products"
        assert all(s.source_type == DataSourceType.POSTGRESQL for s in sources)
        mock_engine.dispose.assert_called_once()
    
    @patch('sqlalchemy.create_engine')
    @patch('sqlalchemy.inspect')
    def test_discover_mysql_sources(self, mock_inspect, mock_create_engine, agent, mysql_config):
        """Test discovering MySQL tables."""
        # Mock SQLAlchemy engine and inspector
        mock_engine = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["customers", "invoices"]
        
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        sources = agent.discover_sources(mysql_config)
        
        assert len(sources) == 2
        assert sources[0].name == "customers"
        assert sources[1].name == "invoices"
        assert all(s.source_type == DataSourceType.MYSQL for s in sources)
        mock_engine.dispose.assert_called_once()
    
    @patch('boto3.client')
    def test_discover_s3_sources(self, mock_boto3_client, agent, s3_config):
        """Test discovering S3 objects."""
        # Mock boto3 S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'data/file1.csv', 'Size': 1024, 'LastModified': datetime.now()},
                {'Key': 'data/file2.json', 'Size': 2048, 'LastModified': datetime.now()},
            ]
        }
        mock_boto3_client.return_value = mock_s3_client
        
        sources = agent.discover_sources(s3_config)
        
        assert len(sources) == 2
        assert sources[0].name == "data/file1.csv"
        assert sources[1].name == "data/file2.json"
        assert all(s.source_type == DataSourceType.S3 for s in sources)
    
    def test_discover_unsupported_source_type(self, agent):
        """Test that unsupported source types raise ValueError."""
        config = ConnectionConfig(source_type="unsupported")
        
        with pytest.raises(ValueError, match="Unsupported source type"):
            agent.discover_sources(config)
    
    @patch('sqlalchemy.create_engine')
    @patch('sqlalchemy.inspect')
    def test_extract_postgresql_metadata(self, mock_inspect, mock_create_engine, agent, postgres_config):
        """Test extracting metadata from PostgreSQL table."""
        # Create a mock data source
        source = DataSource(
            id="pg_testdb_users",
            name="users",
            source_type=DataSourceType.POSTGRESQL,
            connection_config=postgres_config,
            discovered_at=datetime.now(),
            metadata={"database": "testdb", "table": "users"}
        )
        
        # Mock SQLAlchemy components
        mock_engine = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": False, "comment": None},
            {"name": "username", "type": "VARCHAR", "nullable": False, "comment": None},
            {"name": "email", "type": "VARCHAR", "nullable": True, "comment": None},
        ]
        
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.side_effect = [100, 8192]  # row count, size
        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        metadata = agent.extract_metadata(source)
        
        assert isinstance(metadata, SourceMetadata)
        assert metadata.source_id == "pg_testdb_users"
        assert metadata.row_count == 100
        assert metadata.size_bytes == 8192
        assert len(metadata.schema.fields) == 3
        assert metadata.schema.fields[0].name == "id"
        assert metadata.schema.fields[0].data_type == "INTEGER"
        assert metadata.schema.fields[0].nullable is False
    
    @patch('boto3.client')
    def test_extract_s3_metadata(self, mock_boto3_client, agent, s3_config):
        """Test extracting metadata from S3 object."""
        source = DataSource(
            id="s3_test-bucket_data_file.csv",
            name="data/file.csv",
            source_type=DataSourceType.S3,
            connection_config=s3_config,
            discovered_at=datetime.now(),
            metadata={"bucket": "test-bucket", "key": "data/file.csv"}
        )
        
        mock_s3_client = MagicMock()
        mock_s3_client.head_object.return_value = {
            'ContentLength': 5120,
            'LastModified': datetime.now(),
            'ContentType': 'text/csv',
            'ETag': '"abc123"'
        }
        mock_boto3_client.return_value = mock_s3_client
        
        metadata = agent.extract_metadata(source)
        
        assert isinstance(metadata, SourceMetadata)
        assert metadata.source_id == "s3_test-bucket_data_file.csv"
        assert metadata.size_bytes == 5120
        assert metadata.statistics['content_type'] == 'text/csv'
    
    def test_detect_schema_changes_no_cache(self, agent):
        """Test schema change detection with no cached schema."""
        schema = Schema(
            id="test_schema_v1",
            source_id="test_source",
            version=1,
            fields=[Field(name="id", data_type="INTEGER", nullable=False)],
            timestamp=datetime.now()
        )
        
        changes = agent.detect_schema_changes("test_source", schema)
        
        assert len(changes) == 0
        assert "test_source" in agent._schema_cache
    
    def test_detect_schema_changes_added_field(self, agent):
        """Test detection of added fields."""
        old_schema = Schema(
            id="test_schema_v1",
            source_id="test_source",
            version=1,
            fields=[Field(name="id", data_type="INTEGER", nullable=False)],
            timestamp=datetime.now()
        )
        agent._schema_cache["test_source"] = old_schema
        
        new_schema = Schema(
            id="test_schema_v2",
            source_id="test_source",
            version=2,
            fields=[
                Field(name="id", data_type="INTEGER", nullable=False),
                Field(name="email", data_type="VARCHAR", nullable=True)
            ],
            timestamp=datetime.now()
        )
        
        changes = agent.detect_schema_changes("test_source", new_schema)
        
        assert len(changes) == 1
        assert changes[0].change_type == "added"
        assert changes[0].field_name == "email"
        assert changes[0].new_value == "VARCHAR"
    
    def test_detect_schema_changes_removed_field(self, agent):
        """Test detection of removed fields."""
        old_schema = Schema(
            id="test_schema_v1",
            source_id="test_source",
            version=1,
            fields=[
                Field(name="id", data_type="INTEGER", nullable=False),
                Field(name="deprecated", data_type="VARCHAR", nullable=True)
            ],
            timestamp=datetime.now()
        )
        agent._schema_cache["test_source"] = old_schema
        
        new_schema = Schema(
            id="test_schema_v2",
            source_id="test_source",
            version=2,
            fields=[Field(name="id", data_type="INTEGER", nullable=False)],
            timestamp=datetime.now()
        )
        
        changes = agent.detect_schema_changes("test_source", new_schema)
        
        assert len(changes) == 1
        assert changes[0].change_type == "removed"
        assert changes[0].field_name == "deprecated"
        assert changes[0].old_value == "VARCHAR"
    
    def test_detect_schema_changes_type_changed(self, agent):
        """Test detection of field type changes."""
        old_schema = Schema(
            id="test_schema_v1",
            source_id="test_source",
            version=1,
            fields=[Field(name="amount", data_type="INTEGER", nullable=False)],
            timestamp=datetime.now()
        )
        agent._schema_cache["test_source"] = old_schema
        
        new_schema = Schema(
            id="test_schema_v2",
            source_id="test_source",
            version=2,
            fields=[Field(name="amount", data_type="DECIMAL", nullable=False)],
            timestamp=datetime.now()
        )
        
        changes = agent.detect_schema_changes("test_source", new_schema)
        
        assert len(changes) == 1
        assert changes[0].change_type == "type_changed"
        assert changes[0].field_name == "amount"
        assert changes[0].old_value == "INTEGER"
        assert changes[0].new_value == "DECIMAL"
    
    def test_detect_schema_changes_nullable_changed(self, agent):
        """Test detection of nullable constraint changes."""
        old_schema = Schema(
            id="test_schema_v1",
            source_id="test_source",
            version=1,
            fields=[Field(name="email", data_type="VARCHAR", nullable=True)],
            timestamp=datetime.now()
        )
        agent._schema_cache["test_source"] = old_schema
        
        new_schema = Schema(
            id="test_schema_v2",
            source_id="test_source",
            version=2,
            fields=[Field(name="email", data_type="VARCHAR", nullable=False)],
            timestamp=datetime.now()
        )
        
        changes = agent.detect_schema_changes("test_source", new_schema)
        
        assert len(changes) == 1
        assert changes[0].change_type == "modified"
        assert changes[0].field_name == "email"
        assert "nullable=True" in changes[0].old_value
        assert "nullable=False" in changes[0].new_value
    
    def test_update_catalog_publishes_event(self, agent, message_bus):
        """Test that update_catalog publishes discovery event."""
        received_messages = []
        message_bus.subscribe("discovery.events", lambda msg: received_messages.append(msg))
        
        schema = Schema(
            id="test_schema_v1",
            source_id="test_source",
            version=1,
            fields=[Field(name="id", data_type="INTEGER", nullable=False)],
            timestamp=datetime.now()
        )
        
        metadata = SourceMetadata(
            source_id="test_source",
            schema=schema,
            row_count=100,
            size_bytes=1024
        )
        
        agent.update_catalog(metadata)
        
        assert len(received_messages) == 1
        assert received_messages[0].event_type == "data.discovery.completed"
        assert received_messages[0].payload["source_id"] == "test_source"
        assert received_messages[0].payload["row_count"] == 100
        assert received_messages[0].payload["size_bytes"] == 1024
    
    def test_update_catalog_with_schema_changes(self, agent, message_bus):
        """Test that update_catalog publishes schema change events."""
        received_messages = []
        message_bus.subscribe("discovery.events", lambda msg: received_messages.append(msg))
        
        # Set up initial schema
        old_schema = Schema(
            id="test_schema_v1",
            source_id="test_source",
            version=1,
            fields=[Field(name="id", data_type="INTEGER", nullable=False)],
            timestamp=datetime.now()
        )
        agent._schema_cache["test_source"] = old_schema
        
        # Create new schema with changes
        new_schema = Schema(
            id="test_schema_v2",
            source_id="test_source",
            version=2,
            fields=[
                Field(name="id", data_type="INTEGER", nullable=False),
                Field(name="name", data_type="VARCHAR", nullable=True)
            ],
            timestamp=datetime.now()
        )
        
        metadata = SourceMetadata(
            source_id="test_source",
            schema=new_schema,
            row_count=100,
            size_bytes=1024
        )
        
        agent.update_catalog(metadata)
        
        assert len(received_messages) >= 1
        discovery_event = received_messages[0]
        assert discovery_event.event_type == "data.discovery.completed"
        assert len(discovery_event.payload["schema_changes"]) == 1
        assert discovery_event.payload["schema_changes"][0]["change_type"] == "added"
        assert discovery_event.payload["schema_changes"][0]["field_name"] == "name"
