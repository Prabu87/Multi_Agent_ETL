"""Tests for Data Discovery Agent using LangGraph."""

import pytest
from datetime import datetime
from unittest.mock import Mock, MagicMock, patch
from langchain_core.messages import HumanMessage

from etl_platform.agents import DataDiscoveryAgentLangGraph
from etl_platform.shared import (
    ConnectionConfig,
    DataSource,
    DataSourceType,
    Field,
    Schema,
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
    """Provide a LangGraph-based Data Discovery Agent instance."""
    return DataDiscoveryAgentLangGraph(message_bus=message_bus, agent_id="test-langgraph-agent")


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


class TestDataDiscoveryAgentLangGraph:
    """Test suite for LangGraph-based Data Discovery Agent."""
    
    def test_agent_initialization(self, message_bus):
        """Test agent initializes with LangGraph architecture."""
        agent = DataDiscoveryAgentLangGraph(
            message_bus=message_bus,
            agent_id="custom-langgraph-agent"
        )
        assert agent.agent_id == "custom-langgraph-agent"
        assert agent.agent_type == "data-discovery"
        assert agent.message_bus == message_bus
        assert agent.graph is not None
        assert isinstance(agent._schema_cache, dict)
    
    def test_agent_auto_generates_id(self, message_bus):
        """Test agent auto-generates ID if not provided."""
        agent = DataDiscoveryAgentLangGraph(message_bus=message_bus)
        assert agent.agent_id.startswith("data-discovery-")
    
    def test_graph_structure(self, agent):
        """Test that the agent graph is properly constructed."""
        # The graph should be compiled and ready to execute
        assert agent.graph is not None
        # Graph should have the expected nodes
        # Note: LangGraph doesn't expose nodes directly, so we test execution
    
    @patch('sqlalchemy.create_engine')
    @patch('sqlalchemy.inspect')
    def test_discover_and_catalog_postgresql(
        self, mock_inspect, mock_create_engine, agent, postgres_config, message_bus
    ):
        """Test full discovery and catalog workflow for PostgreSQL."""
        # Mock SQLAlchemy components
        mock_engine = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["users"]
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": False, "comment": None},
            {"name": "username", "type": "VARCHAR", "nullable": False, "comment": None},
        ]
        
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.side_effect = [50, 4096]  # row count, size
        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        # Subscribe to events
        received_messages = []
        message_bus.subscribe("discovery.events", lambda msg: received_messages.append(msg))
        
        mock_inspect.return_value = mock_inspector
        result = agent.discover_and_catalog(postgres_config)
        
        # Verify result
        assert result is not None
        assert result["source_id"] == "pg_testdb_users"
        assert result["row_count"] == 50
        assert result["size_bytes"] == 4096
        assert result["field_count"] == 2
        
        # Verify event was published
        assert len(received_messages) == 1
        assert received_messages[0].event_type == "data.discovery.completed"
    
    @patch('sqlalchemy.create_engine')
    @patch('sqlalchemy.inspect')
    def test_schema_change_detection_in_workflow(
        self, mock_inspect, mock_create_engine, agent, postgres_config, message_bus
    ):
        """Test schema change detection within the LangGraph workflow."""
        # First discovery
        mock_engine = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["users"]
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": False, "comment": None},
        ]
        
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.side_effect = [50, 4096, 50, 4096]
        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        
        received_messages = []
        message_bus.subscribe("discovery.events", lambda msg: received_messages.append(msg))
        message_bus.subscribe("schema.events", lambda msg: received_messages.append(msg))
        
        mock_inspect.return_value = mock_inspector
        result1 = agent.discover_and_catalog(postgres_config)
        
        # No changes on first run
        assert len(result1["schema_changes"]) == 0
        
        # Second discovery with schema change
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": False, "comment": None},
            {"name": "email", "type": "VARCHAR", "nullable": True, "comment": None},
        ]
        
        result2 = agent.discover_and_catalog(postgres_config)
        
        # Should detect added field
        assert len(result2["schema_changes"]) == 1
        assert result2["schema_changes"][0]["change_type"] == "added"
        assert result2["schema_changes"][0]["field_name"] == "email"
    
    def test_error_handling_in_workflow(self, agent):
        """Test error handling within the LangGraph workflow."""
        # Invalid configuration should raise an error
        invalid_config = ConnectionConfig(
            source_type="invalid_type",
            host="localhost"
        )
        
        with pytest.raises(Exception):
            agent.discover_and_catalog(invalid_config)
    
    @patch('boto3.client')
    def test_discover_and_catalog_s3(self, mock_boto3_client, agent, message_bus):
        """Test full discovery workflow for S3 sources."""
        s3_config = ConnectionConfig(
            source_type=DataSourceType.S3,
            bucket="test-bucket",
            region="us-east-1"
        )
        
        # Mock boto3 S3 client
        mock_s3_client = MagicMock()
        mock_s3_client.list_objects_v2.return_value = {
            'Contents': [
                {'Key': 'data/file1.csv', 'Size': 1024, 'LastModified': datetime.now()},
            ]
        }
        mock_s3_client.head_object.return_value = {
            'ContentLength': 1024,
            'LastModified': datetime.now(),
            'ContentType': 'text/csv',
            'ETag': '"abc123"'
        }
        mock_boto3_client.return_value = mock_s3_client
        
        received_messages = []
        message_bus.subscribe("discovery.events", lambda msg: received_messages.append(msg))
        
        result = agent.discover_and_catalog(s3_config)
        
        # Verify result
        assert result is not None
        assert "s3_test-bucket" in result["source_id"]
        assert result["size_bytes"] == 1024
        
        # Verify event was published
        assert len(received_messages) == 1
    
    def test_publish_event_method(self, agent, message_bus):
        """Test the publish_event helper method."""
        received_messages = []
        message_bus.subscribe("test.topic", lambda msg: received_messages.append(msg))
        
        agent.publish_event(
            event_type="test.event",
            payload={"key": "value"},
            topic="test.topic"
        )
        
        assert len(received_messages) == 1
        assert received_messages[0].event_type == "test.event"
        assert received_messages[0].payload == {"key": "value"}
        assert received_messages[0].source == agent.agent_id
    
    @patch('sqlalchemy.create_engine')
    @patch('sqlalchemy.inspect')
    def test_state_progression_through_nodes(
        self, mock_inspect, mock_create_engine, agent, postgres_config
    ):
        """Test that state progresses correctly through all nodes."""
        # Mock SQLAlchemy components
        mock_engine = MagicMock()
        mock_inspector = MagicMock()
        mock_inspector.get_table_names.return_value = ["test_table"]
        mock_inspector.get_columns.return_value = [
            {"name": "id", "type": "INTEGER", "nullable": False, "comment": None},
        ]
        
        mock_connection = MagicMock()
        mock_result = MagicMock()
        mock_result.scalar.side_effect = [10, 1024]
        mock_connection.execute.return_value = mock_result
        mock_connection.__enter__ = Mock(return_value=mock_connection)
        mock_connection.__exit__ = Mock(return_value=False)
        
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        result = agent.discover_and_catalog(postgres_config)
        
        # Verify that all nodes executed successfully
        assert result is not None
        assert "source_id" in result
        assert "schema_version" in result
        assert "row_count" in result
        assert "field_count" in result
        assert "schema_changes" in result
    
    @patch('sqlalchemy.create_engine')
    @patch('sqlalchemy.inspect')
    def test_multiple_discoveries_maintain_cache(self, mock_inspect, mock_create_engine, agent, postgres_config):
        """Test that schema cache is maintained across multiple discoveries."""
            mock_engine = MagicMock()
            mock_inspector = MagicMock()
            mock_inspector.get_table_names.return_value = ["users"]
            mock_inspector.get_columns.return_value = [
                {"name": "id", "type": "INTEGER", "nullable": False, "comment": None},
            ]
            
            mock_connection = MagicMock()
            mock_result = MagicMock()
            mock_result.scalar.side_effect = [10, 1024, 10, 1024]
            mock_connection.execute.return_value = mock_result
            mock_connection.__enter__ = Mock(return_value=mock_connection)
            mock_connection.__exit__ = Mock(return_value=False)
            
        mock_engine.connect.return_value = mock_connection
        mock_create_engine.return_value = mock_engine
        mock_inspect.return_value = mock_inspector
        
        # First discovery
        agent.discover_and_catalog(postgres_config)
        
        # Cache should have one entry
        assert len(agent._schema_cache) == 1
        
        # Second discovery
        agent.discover_and_catalog(postgres_config)
        
        # Cache should still have one entry (same source)
        assert len(agent._schema_cache) == 1
