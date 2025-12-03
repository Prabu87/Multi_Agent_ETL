"""Tests for Schema Mapping Agent LangGraph implementation."""

import pytest
from datetime import datetime
from etl_platform.agents import SchemaMappingAgentLangGraph
from etl_platform.shared import (
    Field,
    Schema,
    FieldMapping,
    MappingType,
    TransformationLogic,
    SchemaChange,
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
    """Provide a Schema Mapping Agent LangGraph instance."""
    return SchemaMappingAgentLangGraph(message_bus=message_bus, agent_id="test-mapping-agent-lg")


@pytest.fixture
def source_schema():
    """Provide a sample source schema."""
    return Schema(
        id="source_schema_v1",
        source_id="source_db",
        version=1,
        fields=[
            Field(name="user_id", data_type="INTEGER", nullable=False),
            Field(name="user_name", data_type="VARCHAR(255)", nullable=False),
            Field(name="email_addr", data_type="VARCHAR(255)", nullable=True),
            Field(name="created_at", data_type="TIMESTAMP", nullable=False),
            Field(name="age", data_type="INTEGER", nullable=True),
        ],
        timestamp=datetime.now()
    )


@pytest.fixture
def target_schema():
    """Provide a sample target schema."""
    return Schema(
        id="target_schema_v1",
        source_id="target_db",
        version=1,
        fields=[
            Field(name="id", data_type="BIGINT", nullable=False),
            Field(name="username", data_type="TEXT", nullable=False),
            Field(name="email", data_type="TEXT", nullable=True),
            Field(name="registration_date", data_type="DATE", nullable=False),
        ],
        timestamp=datetime.now()
    )


class TestSchemaMappingAgentLangGraph:
    """Test suite for Schema Mapping Agent LangGraph."""
    
    def test_agent_initialization(self, message_bus):
        """Test agent initializes with correct attributes."""
        agent = SchemaMappingAgentLangGraph(message_bus=message_bus, agent_id="custom-agent")
        assert agent.agent_id == "custom-agent"
        assert agent.message_bus == message_bus
        assert agent.agent_type == "schema-mapping"
        assert isinstance(agent._mapping_cache, dict)
    
    def test_agent_auto_generates_id(self, message_bus):
        """Test agent auto-generates ID if not provided."""
        agent = SchemaMappingAgentLangGraph(message_bus=message_bus)
        assert agent.agent_id.startswith("schema-mapping-")
    
    def test_graph_is_compiled(self, agent):
        """Test that the agent's graph is properly compiled."""
        assert agent.graph is not None
    
    def test_generate_mappings_basic(self, agent, source_schema, target_schema):
        """Test basic mapping generation through LangGraph."""
        mappings = agent.generate_mappings(source_schema, target_schema)
        
        assert len(mappings) > 0
        assert all(isinstance(m, FieldMapping) for m in mappings)
        assert all(0.0 <= m.confidence <= 1.0 for m in mappings)
    
    def test_generate_mappings_finds_similar_names(self, agent, source_schema, target_schema):
        """Test that mapping finds similar field names."""
        mappings = agent.generate_mappings(source_schema, target_schema)
        
        # Should map user_id to id
        id_mapping = next((m for m in mappings if m.target_field == "id"), None)
        assert id_mapping is not None
        assert id_mapping.source_field == "user_id"
        
        # Should map user_name to username
        username_mapping = next((m for m in mappings if m.target_field == "username"), None)
        assert username_mapping is not None
        assert username_mapping.source_field == "user_name"
        
        # Should map email_addr to email
        email_mapping = next((m for m in mappings if m.target_field == "email"), None)
        assert email_mapping is not None
        assert email_mapping.source_field == "email_addr"
    
    def test_generate_mappings_type_conversion(self, agent, source_schema, target_schema):
        """Test that mappings include type conversions when needed."""
        mappings = agent.generate_mappings(source_schema, target_schema)
        
        # user_id (INTEGER) to id (BIGINT) should have transformation
        id_mapping = next((m for m in mappings if m.target_field == "id"), None)
        if id_mapping:
            assert id_mapping.mapping_type == MappingType.TRANSFORMED
            assert id_mapping.transformation is not None
    
    def test_generate_mappings_confidence_scores(self, agent, source_schema, target_schema):
        """Test that confidence scores are calculated correctly."""
        mappings = agent.generate_mappings(source_schema, target_schema)
        
        # Exact or very similar names should have high confidence
        username_mapping = next((m for m in mappings if m.target_field == "username"), None)
        if username_mapping:
            assert username_mapping.confidence > 0.7
    
    def test_generate_mappings_publishes_event(self, agent, message_bus, source_schema, target_schema):
        """Test that mapping generation publishes an event."""
        received_messages = []
        message_bus.subscribe("mapping.events", lambda msg: received_messages.append(msg))
        
        agent.generate_mappings(source_schema, target_schema)
        
        assert len(received_messages) == 1
        assert received_messages[0].event_type == "schema.mapping.generated"
        assert received_messages[0].payload["source_id"] == source_schema.id
        assert received_messages[0].payload["target_id"] == target_schema.id
    
    def test_generate_transformation_direct(self, agent):
        """Test transformation generation for direct mappings."""
        mapping = FieldMapping(
            source_field="name",
            target_field="name",
            mapping_type=MappingType.DIRECT,
            confidence=1.0
        )
        
        transformation = agent.generate_transformation(mapping)
        
        assert isinstance(transformation, TransformationLogic)
        assert transformation.sql_logic == "name"
        assert "name" in transformation.python_logic
        assert "Direct mapping" in transformation.description
    
    def test_generate_transformation_with_conversion(self, agent):
        """Test transformation generation with type conversion."""
        mapping = FieldMapping(
            source_field="age",
            target_field="age",
            transformation="CAST({field} AS VARCHAR)",
            mapping_type=MappingType.TRANSFORMED,
            confidence=0.9
        )
        
        transformation = agent.generate_transformation(mapping)
        
        assert isinstance(transformation, TransformationLogic)
        assert "age" in transformation.sql_logic
        assert "CAST" in transformation.sql_logic or "cast" in transformation.sql_logic.lower()
        assert transformation.python_logic is not None
    
    def test_update_mappings_added_field(self, agent, source_schema, target_schema):
        """Test updating mappings when a field is added."""
        # Generate initial mappings
        initial_mappings = agent.generate_mappings(source_schema, target_schema)
        
        # Create a schema change for an added field
        new_source_schema = Schema(
            id=source_schema.id,
            source_id=source_schema.source_id,
            version=2,
            fields=source_schema.fields + [
                Field(name="phone", data_type="VARCHAR(20)", nullable=True)
            ],
            timestamp=datetime.now()
        )
        
        schema_changes = [
            SchemaChange(
                source_id=source_schema.source_id,
                change_type="added",
                field_name="phone",
                new_value="VARCHAR(20)"
            )
        ]
        
        updated_mappings = agent.update_mappings(schema_changes, new_source_schema, target_schema)
        
        assert isinstance(updated_mappings, list)
        # Should have at least the initial mappings
        assert len(updated_mappings) >= len(initial_mappings)
    
    def test_update_mappings_removed_field(self, agent, source_schema, target_schema):
        """Test updating mappings when a field is removed."""
        # Generate initial mappings
        initial_mappings = agent.generate_mappings(source_schema, target_schema)
        
        # Create a schema with a removed field
        new_source_schema = Schema(
            id=source_schema.id,
            source_id=source_schema.source_id,
            version=2,
            fields=[f for f in source_schema.fields if f.name != "age"],
            timestamp=datetime.now()
        )
        
        schema_changes = [
            SchemaChange(
                source_id=source_schema.source_id,
                change_type="removed",
                field_name="age",
                old_value="INTEGER"
            )
        ]
        
        updated_mappings = agent.update_mappings(schema_changes, new_source_schema, target_schema)
        
        # Should not have a mapping for the removed field
        assert not any(m.source_field == "age" for m in updated_mappings)
    
    def test_update_mappings_type_changed(self, agent, source_schema, target_schema):
        """Test updating mappings when a field type changes."""
        # Generate initial mappings
        initial_mappings = agent.generate_mappings(source_schema, target_schema)
        
        # Create a schema with a changed field type
        new_fields = []
        for field in source_schema.fields:
            if field.name == "age":
                new_fields.append(Field(name="age", data_type="DECIMAL", nullable=True))
            else:
                new_fields.append(field)
        
        new_source_schema = Schema(
            id=source_schema.id,
            source_id=source_schema.source_id,
            version=2,
            fields=new_fields,
            timestamp=datetime.now()
        )
        
        schema_changes = [
            SchemaChange(
                source_id=source_schema.source_id,
                change_type="type_changed",
                field_name="age",
                old_value="INTEGER",
                new_value="DECIMAL"
            )
        ]
        
        updated_mappings = agent.update_mappings(schema_changes, new_source_schema, target_schema)
        
        # Should still have mappings
        assert len(updated_mappings) > 0
    
    def test_update_mappings_publishes_event(self, agent, message_bus, source_schema, target_schema):
        """Test that mapping updates publish an event."""
        # Generate initial mappings
        agent.generate_mappings(source_schema, target_schema)
        
        received_messages = []
        message_bus.subscribe("mapping.events", lambda msg: received_messages.append(msg))
        
        schema_changes = [
            SchemaChange(
                source_id=source_schema.source_id,
                change_type="added",
                field_name="new_field",
                new_value="VARCHAR"
            )
        ]
        
        new_source_schema = Schema(
            id=source_schema.id,
            source_id=source_schema.source_id,
            version=2,
            fields=source_schema.fields + [
                Field(name="new_field", data_type="VARCHAR", nullable=True)
            ],
            timestamp=datetime.now()
        )
        
        agent.update_mappings(schema_changes, new_source_schema, target_schema)
        
        assert len(received_messages) == 1
        assert received_messages[0].event_type == "schema.mapping.updated"
    
    def test_name_similarity_calculation(self, agent):
        """Test name similarity calculation."""
        # Exact match
        assert agent._calculate_name_similarity("user_id", "user_id") == 1.0
        
        # Normalized match
        assert agent._calculate_name_similarity("user_id", "USER_ID") == 1.0
        
        # Partial match
        similarity = agent._calculate_name_similarity("user_id", "id")
        assert similarity > 0.5
        
        # Abbreviation match
        similarity = agent._calculate_name_similarity("id", "identifier")
        assert similarity >= 0.9
    
    def test_type_compatibility_check(self, agent):
        """Test type compatibility checking."""
        # Identical types
        assert agent._check_type_compatibility("INTEGER", "INTEGER")
        
        # Compatible numeric types
        assert agent._check_type_compatibility("INTEGER", "BIGINT")
        
        # Compatible string types
        assert agent._check_type_compatibility("VARCHAR", "TEXT")
        
        # Incompatible types
        assert not agent._check_type_compatibility("INTEGER", "VARCHAR")
    
    def test_type_normalization(self, agent):
        """Test type normalization."""
        assert agent._normalize_type("VARCHAR(255)") == "VARCHAR"
        assert agent._normalize_type("DECIMAL(10,2)") == "DECIMAL"
        assert agent._normalize_type("integer") == "INTEGER"
    
    def test_mapping_caching(self, agent, source_schema, target_schema):
        """Test that mappings are cached correctly."""
        # Generate mappings
        mappings1 = agent.generate_mappings(source_schema, target_schema)
        
        # Check cache
        cache_key = f"{source_schema.id}_{target_schema.id}"
        assert cache_key in agent._mapping_cache
        assert len(agent._mapping_cache[cache_key]) == len(mappings1)
