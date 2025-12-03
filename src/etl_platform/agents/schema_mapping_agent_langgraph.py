"""Schema Mapping Agent using LangChain and LangGraph."""

import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any, TypedDict
import logging
import difflib
import re

from langchain_core.messages import BaseMessage, HumanMessage, AIMessage, SystemMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, END

from etl_platform.shared.models import (
    Schema,
    Field,
    FieldMapping,
    MappingType,
    TransformationLogic,
    SchemaChange,
)
from etl_platform.shared.message_bus import MessageBus
from etl_platform.agents.base_agent import BaseAgent, AgentState


logger = logging.getLogger(__name__)


class SchemaMappingState(AgentState):
    """State for schema mapping agent execution."""
    source_schema: Optional[Schema]
    target_schema: Optional[Schema]
    schema_changes: Optional[List[SchemaChange]]
    mappings: List[FieldMapping]
    mapping_cache: Dict[str, List[FieldMapping]]


class SchemaMappingAgentLangGraph(BaseAgent):
    """Agent responsible for automatic schema mapping using LangGraph."""
    
    # Data type compatibility matrix
    TYPE_COMPATIBILITY = {
        # String types
        'VARCHAR': ['TEXT', 'CHAR', 'STRING', 'VARCHAR'],
        'TEXT': ['VARCHAR', 'CHAR', 'STRING', 'TEXT'],
        'CHAR': ['VARCHAR', 'TEXT', 'STRING', 'CHAR'],
        'STRING': ['VARCHAR', 'TEXT', 'CHAR', 'STRING'],
        
        # Numeric types
        'INTEGER': ['BIGINT', 'SMALLINT', 'INT', 'INTEGER', 'NUMERIC', 'DECIMAL'],
        'BIGINT': ['INTEGER', 'SMALLINT', 'INT', 'BIGINT', 'NUMERIC', 'DECIMAL'],
        'SMALLINT': ['INTEGER', 'BIGINT', 'INT', 'SMALLINT', 'NUMERIC', 'DECIMAL'],
        'INT': ['INTEGER', 'BIGINT', 'SMALLINT', 'INT', 'NUMERIC', 'DECIMAL'],
        'NUMERIC': ['INTEGER', 'BIGINT', 'DECIMAL', 'FLOAT', 'DOUBLE', 'NUMERIC'],
        'DECIMAL': ['INTEGER', 'BIGINT', 'NUMERIC', 'FLOAT', 'DOUBLE', 'DECIMAL'],
        'FLOAT': ['DOUBLE', 'NUMERIC', 'DECIMAL', 'FLOAT'],
        'DOUBLE': ['FLOAT', 'NUMERIC', 'DECIMAL', 'DOUBLE'],
        
        # Date/Time types
        'DATE': ['TIMESTAMP', 'DATETIME', 'DATE'],
        'TIMESTAMP': ['DATETIME', 'DATE', 'TIMESTAMP'],
        'DATETIME': ['TIMESTAMP', 'DATE', 'DATETIME'],
        'TIME': ['TIME'],
        
        # Boolean
        'BOOLEAN': ['BOOL', 'BIT', 'BOOLEAN'],
        'BOOL': ['BOOLEAN', 'BIT', 'BOOL'],
        
        # Binary
        'BLOB': ['BINARY', 'VARBINARY', 'BLOB'],
        'BINARY': ['BLOB', 'VARBINARY', 'BINARY'],
    }
    
    def __init__(
        self,
        message_bus: MessageBus,
        agent_id: Optional[str] = None
    ):
        """
        Initialize the Schema Mapping Agent.
        
        Args:
            message_bus: Message bus for publishing mapping events
            agent_id: Unique identifier for this agent instance
        """
        super().__init__(
            message_bus=message_bus,
            agent_id=agent_id,
            agent_type="schema-mapping"
        )
        self._mapping_cache: Dict[str, List[FieldMapping]] = {}
    
    def _build_graph(self) -> StateGraph:
        """
        Build the agent's execution graph for schema mapping.
        
        Returns:
            Compiled StateGraph for schema mapping
        """
        workflow = StateGraph(SchemaMappingState)
        
        # Add nodes
        workflow.add_node("validate_input", self._validate_input)
        workflow.add_node("analyze_schemas", self._analyze_schemas)
        workflow.add_node("generate_mappings", self._generate_mappings_node)
        workflow.add_node("calculate_confidence", self._calculate_confidence_node)
        workflow.add_node("update_mappings", self._update_mappings_node)
        workflow.add_node("publish_results", self._publish_results)
        
        # Define edges
        workflow.set_entry_point("validate_input")
        
        workflow.add_conditional_edges(
            "validate_input",
            self._route_after_validation,
            {
                "generate": "analyze_schemas",
                "update": "update_mappings",
                "error": END
            }
        )
        
        workflow.add_edge("analyze_schemas", "generate_mappings")
        workflow.add_edge("generate_mappings", "calculate_confidence")
        workflow.add_edge("calculate_confidence", "publish_results")
        workflow.add_edge("update_mappings", "publish_results")
        workflow.add_edge("publish_results", END)
        
        return workflow.compile()
    
    def _validate_input(self, state: SchemaMappingState) -> SchemaMappingState:
        """Validate input state and extract schemas."""
        logger.info("Validating input for schema mapping")
        
        context = state.get("context", {})
        
        # Extract schemas from context
        source_schema = context.get("source_schema")
        target_schema = context.get("target_schema")
        schema_changes = context.get("schema_changes")
        
        if schema_changes:
            state["schema_changes"] = schema_changes
            logger.info(f"Processing {len(schema_changes)} schema changes")
        
        if source_schema and target_schema:
            state["source_schema"] = source_schema
            state["target_schema"] = target_schema
            state["mappings"] = []
            state["mapping_cache"] = self._mapping_cache
            logger.info(f"Validated schemas: {source_schema.id} -> {target_schema.id}")
        else:
            state["error"] = "Missing source or target schema"
            logger.error("Validation failed: missing schemas")
        
        return state
    
    def _route_after_validation(self, state: SchemaMappingState) -> str:
        """Route to appropriate node after validation."""
        if state.get("error"):
            return "error"
        elif state.get("schema_changes"):
            return "update"
        else:
            return "generate"
    
    def _analyze_schemas(self, state: SchemaMappingState) -> SchemaMappingState:
        """Analyze source and target schemas."""
        logger.info("Analyzing schemas for mapping")
        
        source = state["source_schema"]
        target = state["target_schema"]
        
        # Add analysis message
        analysis_msg = AIMessage(
            content=f"Analyzing {len(source.fields)} source fields and {len(target.fields)} target fields"
        )
        state["messages"].append(analysis_msg)
        
        # Store field statistics in context
        state["context"]["source_field_count"] = len(source.fields)
        state["context"]["target_field_count"] = len(target.fields)
        
        logger.info(f"Schema analysis complete: {len(source.fields)} -> {len(target.fields)} fields")
        return state
    
    def _generate_mappings_node(self, state: SchemaMappingState) -> SchemaMappingState:
        """Generate field mappings between schemas."""
        logger.info("Generating field mappings")
        
        source = state["source_schema"]
        target = state["target_schema"]
        
        mappings = []
        mapped_source_fields = set()
        
        for target_field in target.fields:
            best_mapping = None
            best_confidence = 0.0
            
            for source_field in source.fields:
                if source_field.name in mapped_source_fields:
                    continue
                
                # Calculate similarity and type compatibility
                name_similarity = self._calculate_name_similarity(
                    source_field.name,
                    target_field.name
                )
                type_compatible = self._check_type_compatibility(
                    source_field.data_type,
                    target_field.data_type
                )
                
                # Calculate overall confidence
                confidence = self._calculate_confidence_score(
                    name_similarity,
                    type_compatible,
                    source_field,
                    target_field
                )
                
                if confidence > best_confidence:
                    best_confidence = confidence
                    
                    # Determine mapping type
                    mapping_type = MappingType.DIRECT
                    transformation = None
                    
                    if not self._are_types_identical(
                        source_field.data_type,
                        target_field.data_type
                    ):
                        mapping_type = MappingType.TRANSFORMED
                        transformation = self._generate_type_conversion(
                            source_field.data_type,
                            target_field.data_type
                        )
                    
                    best_mapping = FieldMapping(
                        source_field=source_field.name,
                        target_field=target_field.name,
                        transformation=transformation,
                        confidence=confidence,
                        mapping_type=mapping_type
                    )
            
            if best_mapping and best_confidence > 0.3:  # Minimum confidence threshold
                mappings.append(best_mapping)
                mapped_source_fields.add(best_mapping.source_field)
                logger.debug(
                    f"Mapped {best_mapping.source_field} -> {best_mapping.target_field} "
                    f"(confidence: {best_confidence:.2f})"
                )
        
        state["mappings"] = mappings
        
        # Cache the mappings
        cache_key = f"{source.id}_{target.id}"
        self._mapping_cache[cache_key] = mappings
        
        # Add result message
        result_msg = AIMessage(
            content=f"Generated {len(mappings)} field mappings with confidence scores"
        )
        state["messages"].append(result_msg)
        
        logger.info(f"Generated {len(mappings)} mappings")
        return state
    
    def _calculate_confidence_node(self, state: SchemaMappingState) -> SchemaMappingState:
        """Calculate and validate confidence scores for mappings."""
        logger.info("Calculating confidence scores")
        
        mappings = state["mappings"]
        
        # Validate all confidence scores are in valid range
        for mapping in mappings:
            if not (0.0 <= mapping.confidence <= 1.0):
                logger.warning(f"Invalid confidence score for {mapping.source_field}: {mapping.confidence}")
                mapping.confidence = max(0.0, min(1.0, mapping.confidence))
        
        # Calculate statistics
        if mappings:
            avg_confidence = sum(m.confidence for m in mappings) / len(mappings)
            high_confidence_count = sum(1 for m in mappings if m.confidence > 0.8)
            
            state["context"]["avg_confidence"] = avg_confidence
            state["context"]["high_confidence_count"] = high_confidence_count
            
            logger.info(f"Average confidence: {avg_confidence:.2f}, High confidence mappings: {high_confidence_count}")
        
        return state
    
    def _update_mappings_node(self, state: SchemaMappingState) -> SchemaMappingState:
        """Update mappings based on schema changes."""
        logger.info("Updating mappings for schema changes")
        
        schema_changes = state["schema_changes"]
        source = state["source_schema"]
        target = state["target_schema"]
        
        cache_key = f"{source.id}_{target.id}"
        existing_mappings = self._mapping_cache.get(cache_key, [])
        
        # Create a map of existing mappings by source field
        mapping_dict = {m.source_field: m for m in existing_mappings}
        
        # Process each schema change
        for change in schema_changes:
            if change.change_type == "added":
                self._process_added_field(change, source, target, mapping_dict, existing_mappings)
            elif change.change_type == "removed":
                self._process_removed_field(change, mapping_dict)
            elif change.change_type == "type_changed":
                self._process_type_changed_field(change, source, target, mapping_dict)
        
        # Convert back to list
        updated_mappings = list(mapping_dict.values())
        state["mappings"] = updated_mappings
        
        # Update cache
        self._mapping_cache[cache_key] = updated_mappings
        
        # Add result message
        result_msg = AIMessage(
            content=f"Updated mappings: {len(updated_mappings)} total mappings after {len(schema_changes)} changes"
        )
        state["messages"].append(result_msg)
        
        logger.info(f"Updated mappings: {len(updated_mappings)} total mappings")
        return state
    
    def _process_added_field(
        self,
        change: SchemaChange,
        source: Schema,
        target: Schema,
        mapping_dict: Dict[str, FieldMapping],
        existing_mappings: List[FieldMapping]
    ) -> None:
        """Process an added field schema change."""
        logger.info(f"Processing added field: {change.field_name}")
        
        new_field = next(
            (f for f in source.fields if f.name == change.field_name),
            None
        )
        
        if new_field:
            for target_field in target.fields:
                if target_field.name not in [m.target_field for m in existing_mappings]:
                    name_similarity = self._calculate_name_similarity(
                        new_field.name,
                        target_field.name
                    )
                    type_compatible = self._check_type_compatibility(
                        new_field.data_type,
                        target_field.data_type
                    )
                    
                    confidence = self._calculate_confidence_score(
                        name_similarity,
                        type_compatible,
                        new_field,
                        target_field
                    )
                    
                    if confidence > 0.5:  # Higher threshold for new mappings
                        mapping_type = MappingType.DIRECT
                        transformation = None
                        
                        if not self._are_types_identical(
                            new_field.data_type,
                            target_field.data_type
                        ):
                            mapping_type = MappingType.TRANSFORMED
                            transformation = self._generate_type_conversion(
                                new_field.data_type,
                                target_field.data_type
                            )
                        
                        new_mapping = FieldMapping(
                            source_field=new_field.name,
                            target_field=target_field.name,
                            transformation=transformation,
                            confidence=confidence,
                            mapping_type=mapping_type
                        )
                        
                        mapping_dict[new_field.name] = new_mapping
                        logger.info(f"Created new mapping for added field: {change.field_name}")
                        break
    
    def _process_removed_field(
        self,
        change: SchemaChange,
        mapping_dict: Dict[str, FieldMapping]
    ) -> None:
        """Process a removed field schema change."""
        if change.field_name in mapping_dict:
            del mapping_dict[change.field_name]
            logger.info(f"Removed mapping for deleted field: {change.field_name}")
    
    def _process_type_changed_field(
        self,
        change: SchemaChange,
        source: Schema,
        target: Schema,
        mapping_dict: Dict[str, FieldMapping]
    ) -> None:
        """Process a type changed field schema change."""
        if change.field_name in mapping_dict:
            old_mapping = mapping_dict[change.field_name]
            
            updated_field = next(
                (f for f in source.fields if f.name == change.field_name),
                None
            )
            
            target_field = next(
                (f for f in target.fields if f.name == old_mapping.target_field),
                None
            )
            
            if updated_field and target_field:
                name_similarity = self._calculate_name_similarity(
                    updated_field.name,
                    target_field.name
                )
                type_compatible = self._check_type_compatibility(
                    updated_field.data_type,
                    target_field.data_type
                )
                
                confidence = self._calculate_confidence_score(
                    name_similarity,
                    type_compatible,
                    updated_field,
                    target_field
                )
                
                mapping_type = MappingType.DIRECT
                transformation = None
                
                if not self._are_types_identical(
                    updated_field.data_type,
                    target_field.data_type
                ):
                    mapping_type = MappingType.TRANSFORMED
                    transformation = self._generate_type_conversion(
                        updated_field.data_type,
                        target_field.data_type
                    )
                
                updated_mapping = FieldMapping(
                    source_field=updated_field.name,
                    target_field=target_field.name,
                    transformation=transformation,
                    confidence=confidence,
                    mapping_type=mapping_type
                )
                
                mapping_dict[change.field_name] = updated_mapping
                logger.info(f"Updated mapping for type-changed field: {change.field_name}")
    
    def _publish_results(self, state: SchemaMappingState) -> SchemaMappingState:
        """Publish mapping results to message bus."""
        logger.info("Publishing mapping results")
        
        mappings = state["mappings"]
        source = state["source_schema"]
        target = state["target_schema"]
        schema_changes = state.get("schema_changes")
        
        if schema_changes:
            # Publish mapping update event
            event_payload = {
                "source_id": source.id,
                "target_id": target.id,
                "change_count": len(schema_changes),
                "mapping_count": len(mappings),
                "changes": [
                    {
                        "change_type": c.change_type,
                        "field_name": c.field_name
                    }
                    for c in schema_changes
                ]
            }
            self.publish_event("schema.mapping.updated", event_payload, "mapping.events")
        else:
            # Publish mapping generation event
            event_payload = {
                "source_id": source.id,
                "target_id": target.id,
                "mapping_count": len(mappings),
                "mappings": [
                    {
                        "source_field": m.source_field,
                        "target_field": m.target_field,
                        "confidence": m.confidence,
                        "mapping_type": m.mapping_type.value,
                        "has_transformation": m.transformation is not None
                    }
                    for m in mappings
                ]
            }
            self.publish_event("schema.mapping.generated", event_payload, "mapping.events")
        
        # Store result
        state["result"] = {
            "mappings": mappings,
            "mapping_count": len(mappings),
            "avg_confidence": state["context"].get("avg_confidence", 0.0)
        }
        
        logger.info("Results published successfully")
        return state
    
    # Helper methods for field name similarity and type compatibility
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """Calculate similarity between two field names."""
        norm1 = name1.lower().replace('_', '').replace('-', '')
        norm2 = name2.lower().replace('_', '').replace('-', '')
        
        if norm1 == norm2:
            return 1.0
        
        similarity = difflib.SequenceMatcher(None, norm1, norm2).ratio()
        
        if norm1 in norm2 or norm2 in norm1:
            similarity = max(similarity, 0.8)
        
        common_abbreviations = {
            'id': 'identifier',
            'num': 'number',
            'qty': 'quantity',
            'amt': 'amount',
            'desc': 'description',
            'addr': 'address',
            'tel': 'telephone',
            'email': 'emailaddress',
        }
        
        for abbr, full in common_abbreviations.items():
            if (norm1 == abbr and norm2 == full) or (norm1 == full and norm2 == abbr):
                similarity = max(similarity, 0.9)
        
        return similarity
    
    def _check_type_compatibility(self, source_type: str, target_type: str) -> bool:
        """Check if source type is compatible with target type."""
        source_normalized = self._normalize_type(source_type)
        target_normalized = self._normalize_type(target_type)
        
        if source_normalized == target_normalized:
            return True
        
        if source_normalized in self.TYPE_COMPATIBILITY:
            return target_normalized in self.TYPE_COMPATIBILITY[source_normalized]
        
        return False
    
    def _normalize_type(self, data_type: str) -> str:
        """Normalize a data type string for comparison."""
        normalized = data_type.upper()
        normalized = re.sub(r'\([^)]*\)', '', normalized)
        normalized = normalized.strip()
        return normalized
    
    def _are_types_identical(self, source_type: str, target_type: str) -> bool:
        """Check if two types are identical."""
        return self._normalize_type(source_type) == self._normalize_type(target_type)
    
    def _generate_type_conversion(self, source_type: str, target_type: str) -> str:
        """Generate a simple type conversion expression."""
        target_norm = self._normalize_type(target_type)
        
        if target_norm in ['VARCHAR', 'TEXT', 'CHAR', 'STRING']:
            return "CAST({field} AS VARCHAR)"
        
        if target_norm in ['INTEGER', 'INT', 'BIGINT', 'SMALLINT']:
            return "CAST({field} AS INTEGER)"
        
        if target_norm in ['NUMERIC', 'DECIMAL']:
            return "CAST({field} AS NUMERIC)"
        
        if target_norm in ['FLOAT', 'DOUBLE']:
            return "CAST({field} AS FLOAT)"
        
        if target_norm in ['DATE', 'TIMESTAMP', 'DATETIME']:
            return "CAST({field} AS TIMESTAMP)"
        
        if target_norm in ['BOOLEAN', 'BOOL']:
            return "CAST({field} AS BOOLEAN)"
        
        return f"CAST({{field}} AS {target_norm})"
    
    def _calculate_confidence_score(
        self,
        name_similarity: float,
        type_compatible: bool,
        source_field: Field,
        target_field: Field
    ) -> float:
        """Calculate overall confidence score for a mapping."""
        confidence = name_similarity
        
        if not type_compatible:
            confidence *= 0.3
        
        if self._are_types_identical(source_field.data_type, target_field.data_type):
            confidence = min(1.0, confidence * 1.2)
        
        if source_field.nullable == target_field.nullable:
            confidence = min(1.0, confidence * 1.05)
        
        if not source_field.nullable and target_field.nullable:
            confidence *= 0.95
        
        if source_field.nullable and not target_field.nullable:
            confidence *= 0.85
        
        return min(1.0, max(0.0, confidence))
    
    # Public API methods
    
    def generate_mappings(self, source: Schema, target: Schema) -> List[FieldMapping]:
        """
        Generate field mappings between source and target schemas.
        
        Args:
            source: Source schema
            target: Target schema
            
        Returns:
            List of field mappings with confidence scores
        """
        initial_state = SchemaMappingState(
            messages=[HumanMessage(content=f"Generate mappings from {source.id} to {target.id}")],
            task_id=f"mapping-{uuid.uuid4().hex[:8]}",
            context={
                "source_schema": source,
                "target_schema": target
            },
            result=None,
            error=None,
            source_schema=source,
            target_schema=target,
            schema_changes=None,
            mappings=[],
            mapping_cache=self._mapping_cache
        )
        
        final_state = self.execute(initial_state)
        
        if final_state.get("error"):
            logger.error(f"Mapping generation failed: {final_state['error']}")
            return []
        
        return final_state.get("result", {}).get("mappings", [])
    
    def update_mappings(
        self,
        schema_changes: List[SchemaChange],
        source: Schema,
        target: Schema
    ) -> List[FieldMapping]:
        """
        Update field mappings based on schema changes.
        
        Args:
            schema_changes: List of detected schema changes
            source: Updated source schema
            target: Target schema
            
        Returns:
            Updated list of field mappings
        """
        initial_state = SchemaMappingState(
            messages=[HumanMessage(content=f"Update mappings for {len(schema_changes)} changes")],
            task_id=f"update-{uuid.uuid4().hex[:8]}",
            context={
                "source_schema": source,
                "target_schema": target,
                "schema_changes": schema_changes
            },
            result=None,
            error=None,
            source_schema=source,
            target_schema=target,
            schema_changes=schema_changes,
            mappings=[],
            mapping_cache=self._mapping_cache
        )
        
        final_state = self.execute(initial_state)
        
        if final_state.get("error"):
            logger.error(f"Mapping update failed: {final_state['error']}")
            return []
        
        return final_state.get("result", {}).get("mappings", [])
    
    def generate_transformation(self, mapping: FieldMapping) -> TransformationLogic:
        """
        Generate detailed transformation logic for a field mapping.
        
        Args:
            mapping: Field mapping to generate transformation for
            
        Returns:
            Transformation logic with SQL and Python implementations
        """
        logger.info(f"Generating transformation for {mapping.source_field} -> {mapping.target_field}")
        
        sql_logic = None
        python_logic = None
        description = ""
        
        if mapping.mapping_type == MappingType.DIRECT:
            sql_logic = f"{mapping.source_field}"
            python_logic = f"row['{mapping.source_field}']"
            description = f"Direct mapping from {mapping.source_field} to {mapping.target_field}"
        
        elif mapping.mapping_type == MappingType.TRANSFORMED:
            if mapping.transformation:
                sql_logic = mapping.transformation.replace('{field}', mapping.source_field)
                
                match = re.search(r'AS\s+(\w+)', mapping.transformation, re.IGNORECASE)
                if match:
                    target_type = match.group(1).upper()
                    python_logic = self._generate_python_conversion(
                        mapping.source_field,
                        target_type
                    )
                
                description = f"Type conversion from {mapping.source_field} to {mapping.target_field}"
        
        else:  # DERIVED
            sql_logic = f"-- Custom logic needed for {mapping.target_field}"
            python_logic = f"# Custom logic needed for {mapping.target_field}"
            description = f"Derived field {mapping.target_field} requires custom logic"
        
        transformation = TransformationLogic(
            mapping=mapping,
            sql_logic=sql_logic,
            python_logic=python_logic,
            description=description
        )
        
        logger.debug(f"Generated transformation: {description}")
        return transformation
    
    def _generate_python_conversion(self, field_name: str, target_type: str) -> str:
        """Generate Python code for type conversion."""
        conversions = {
            'INTEGER': f"int(row['{field_name}'])",
            'INT': f"int(row['{field_name}'])",
            'BIGINT': f"int(row['{field_name}'])",
            'SMALLINT': f"int(row['{field_name}'])",
            'FLOAT': f"float(row['{field_name}'])",
            'DOUBLE': f"float(row['{field_name}'])",
            'VARCHAR': f"str(row['{field_name}'])",
            'TEXT': f"str(row['{field_name}'])",
            'STRING': f"str(row['{field_name}'])",
            'BOOLEAN': f"bool(row['{field_name}'])",
            'BOOL': f"bool(row['{field_name}'])",
        }
        
        return conversions.get(target_type, f"row['{field_name}']")
