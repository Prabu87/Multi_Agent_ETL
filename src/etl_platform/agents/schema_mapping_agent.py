"""Schema Mapping Agent for automatic field mapping between schemas."""

import uuid
from datetime import datetime
from typing import List, Optional, Dict, Any, Tuple
import logging
import difflib
import re

from etl_platform.shared.models import (
    Schema,
    Field,
    FieldMapping,
    MappingType,
    TransformationLogic,
    SchemaChange,
)
from etl_platform.shared.message_bus import Message, MessageBus


logger = logging.getLogger(__name__)


class SchemaMappingAgent:
    """Agent responsible for automatic schema mapping and transformation generation."""
    
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
    
    def __init__(self, message_bus: MessageBus, agent_id: Optional[str] = None):
        """
        Initialize the Schema Mapping Agent.
        
        Args:
            message_bus: Message bus for publishing mapping events
            agent_id: Unique identifier for this agent instance
        """
        self.message_bus = message_bus
        self.agent_id = agent_id or f"schema-mapping-{uuid.uuid4().hex[:8]}"
        self._mapping_cache: Dict[str, List[FieldMapping]] = {}
        logger.info(f"Schema Mapping Agent initialized: {self.agent_id}")
    
    def generate_mappings(
        self,
        source: Schema,
        target: Schema
    ) -> List[FieldMapping]:
        """
        Generate field mappings between source and target schemas.
        
        Args:
            source: Source schema
            target: Target schema
            
        Returns:
            List of field mappings with confidence scores
        """
        logger.info(f"Generating mappings from {source.id} to {target.id}")
        
        mappings = []
        
        # Create a set of already mapped source fields to avoid duplicates
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
                confidence = self._calculate_confidence(
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
        
        # Cache the mappings
        cache_key = f"{source.id}_{target.id}"
        self._mapping_cache[cache_key] = mappings
        
        logger.info(f"Generated {len(mappings)} mappings")
        
        # Publish mapping event
        self._publish_mapping_event(source.id, target.id, mappings)
        
        return mappings
    
    def _calculate_name_similarity(self, name1: str, name2: str) -> float:
        """
        Calculate similarity between two field names.
        
        Args:
            name1: First field name
            name2: Second field name
            
        Returns:
            Similarity score between 0.0 and 1.0
        """
        # Normalize names (lowercase, remove underscores)
        norm1 = name1.lower().replace('_', '').replace('-', '')
        norm2 = name2.lower().replace('_', '').replace('-', '')
        
        # Exact match after normalization
        if norm1 == norm2:
            return 1.0
        
        # Use SequenceMatcher for fuzzy matching
        similarity = difflib.SequenceMatcher(None, norm1, norm2).ratio()
        
        # Boost score if one name contains the other
        if norm1 in norm2 or norm2 in norm1:
            similarity = max(similarity, 0.8)
        
        # Check for common patterns (e.g., id vs identifier, num vs number)
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
        """
        Check if source type is compatible with target type.
        
        Args:
            source_type: Source data type
            target_type: Target data type
            
        Returns:
            True if types are compatible, False otherwise
        """
        # Normalize type names (uppercase, remove size specifications)
        source_normalized = self._normalize_type(source_type)
        target_normalized = self._normalize_type(target_type)
        
        # Check if types are identical
        if source_normalized == target_normalized:
            return True
        
        # Check compatibility matrix
        if source_normalized in self.TYPE_COMPATIBILITY:
            return target_normalized in self.TYPE_COMPATIBILITY[source_normalized]
        
        return False
    
    def _normalize_type(self, data_type: str) -> str:
        """
        Normalize a data type string for comparison.
        
        Args:
            data_type: Data type string
            
        Returns:
            Normalized type string
        """
        # Convert to uppercase
        normalized = data_type.upper()
        
        # Remove size specifications (e.g., VARCHAR(255) -> VARCHAR)
        normalized = re.sub(r'\([^)]*\)', '', normalized)
        
        # Remove whitespace
        normalized = normalized.strip()
        
        return normalized
    
    def _are_types_identical(self, source_type: str, target_type: str) -> bool:
        """
        Check if two types are identical (not just compatible).
        
        Args:
            source_type: Source data type
            target_type: Target data type
            
        Returns:
            True if types are identical
        """
        return self._normalize_type(source_type) == self._normalize_type(target_type)
    
    def _generate_type_conversion(self, source_type: str, target_type: str) -> str:
        """
        Generate a simple type conversion expression.
        
        Args:
            source_type: Source data type
            target_type: Target data type
            
        Returns:
            Conversion expression (placeholder for field name)
        """
        source_norm = self._normalize_type(source_type)
        target_norm = self._normalize_type(target_type)
        
        # String conversions
        if target_norm in ['VARCHAR', 'TEXT', 'CHAR', 'STRING']:
            return "CAST({field} AS VARCHAR)"
        
        # Numeric conversions
        if target_norm in ['INTEGER', 'INT', 'BIGINT', 'SMALLINT']:
            return "CAST({field} AS INTEGER)"
        
        if target_norm in ['NUMERIC', 'DECIMAL']:
            return "CAST({field} AS NUMERIC)"
        
        if target_norm in ['FLOAT', 'DOUBLE']:
            return "CAST({field} AS FLOAT)"
        
        # Date/Time conversions
        if target_norm in ['DATE', 'TIMESTAMP', 'DATETIME']:
            return "CAST({field} AS TIMESTAMP)"
        
        # Boolean conversions
        if target_norm in ['BOOLEAN', 'BOOL']:
            return "CAST({field} AS BOOLEAN)"
        
        # Default: generic cast
        return f"CAST({{field}} AS {target_norm})"
    
    def calculate_confidence(self, mapping: FieldMapping) -> float:
        """
        Calculate confidence score for a field mapping.
        
        This is a public wrapper that can be used to recalculate confidence
        for an existing mapping.
        
        Args:
            mapping: Field mapping to calculate confidence for
            
        Returns:
            Confidence score between 0.0 and 1.0
        """
        # For this simplified version, we'll use the name similarity
        # In a real implementation, this would consider more factors
        name_similarity = self._calculate_name_similarity(
            mapping.source_field,
            mapping.target_field
        )
        
        # Adjust based on mapping type
        if mapping.mapping_type == MappingType.DIRECT:
            return name_similarity
        elif mapping.mapping_type == MappingType.TRANSFORMED:
            return name_similarity * 0.9  # Slight penalty for transformations
        else:  # DERIVED
            return name_similarity * 0.7  # Larger penalty for derived fields
    
    def _calculate_confidence(
        self,
        name_similarity: float,
        type_compatible: bool,
        source_field: Field,
        target_field: Field
    ) -> float:
        """
        Calculate overall confidence score for a mapping.
        
        Args:
            name_similarity: Name similarity score (0.0 to 1.0)
            type_compatible: Whether types are compatible
            source_field: Source field
            target_field: Target field
            
        Returns:
            Overall confidence score between 0.0 and 1.0
        """
        # Start with name similarity
        confidence = name_similarity
        
        # Type compatibility is critical
        if not type_compatible:
            confidence *= 0.3  # Heavy penalty for incompatible types
        
        # Boost confidence if types are identical
        if self._are_types_identical(source_field.data_type, target_field.data_type):
            confidence = min(1.0, confidence * 1.2)
        
        # Consider nullability
        if source_field.nullable == target_field.nullable:
            confidence = min(1.0, confidence * 1.05)
        
        # Penalty if source is not nullable but target is
        if not source_field.nullable and target_field.nullable:
            confidence *= 0.95
        
        # Penalty if source is nullable but target is not (data loss risk)
        if source_field.nullable and not target_field.nullable:
            confidence *= 0.85
        
        return min(1.0, max(0.0, confidence))
    
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
            # Direct mapping - no transformation needed
            sql_logic = f"{mapping.source_field}"
            python_logic = f"row['{mapping.source_field}']"
            description = f"Direct mapping from {mapping.source_field} to {mapping.target_field}"
        
        elif mapping.mapping_type == MappingType.TRANSFORMED:
            # Use the transformation from the mapping
            if mapping.transformation:
                sql_logic = mapping.transformation.replace('{field}', mapping.source_field)
                
                # Generate Python equivalent
                if 'CAST' in mapping.transformation.upper():
                    # Extract target type from CAST expression
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
        """
        Generate Python code for type conversion.
        
        Args:
            field_name: Name of the field to convert
            target_type: Target data type
            
        Returns:
            Python conversion expression
        """
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
        logger.info(f"Updating mappings for {len(schema_changes)} schema changes")
        
        cache_key = f"{source.id}_{target.id}"
        existing_mappings = self._mapping_cache.get(cache_key, [])
        
        # Create a map of existing mappings by source field
        mapping_dict = {m.source_field: m for m in existing_mappings}
        
        # Process each schema change
        for change in schema_changes:
            if change.change_type == "added":
                # Try to find a mapping for the new field
                logger.info(f"Processing added field: {change.field_name}")
                
                # Find the new field in the source schema
                new_field = next(
                    (f for f in source.fields if f.name == change.field_name),
                    None
                )
                
                if new_field:
                    # Try to map to target fields
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
                            
                            confidence = self._calculate_confidence(
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
            
            elif change.change_type == "removed":
                # Remove mapping for deleted field
                if change.field_name in mapping_dict:
                    del mapping_dict[change.field_name]
                    logger.info(f"Removed mapping for deleted field: {change.field_name}")
            
            elif change.change_type == "type_changed":
                # Update mapping for changed field type
                if change.field_name in mapping_dict:
                    old_mapping = mapping_dict[change.field_name]
                    
                    # Find the updated field
                    updated_field = next(
                        (f for f in source.fields if f.name == change.field_name),
                        None
                    )
                    
                    # Find the target field
                    target_field = next(
                        (f for f in target.fields if f.name == old_mapping.target_field),
                        None
                    )
                    
                    if updated_field and target_field:
                        # Recalculate mapping with new type
                        name_similarity = self._calculate_name_similarity(
                            updated_field.name,
                            target_field.name
                        )
                        type_compatible = self._check_type_compatibility(
                            updated_field.data_type,
                            target_field.data_type
                        )
                        
                        confidence = self._calculate_confidence(
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
        
        # Convert back to list
        updated_mappings = list(mapping_dict.values())
        
        # Update cache
        self._mapping_cache[cache_key] = updated_mappings
        
        # Publish mapping update event
        self._publish_mapping_update_event(source.id, target.id, schema_changes, updated_mappings)
        
        logger.info(f"Updated mappings: {len(updated_mappings)} total mappings")
        return updated_mappings
    
    def _publish_mapping_event(
        self,
        source_id: str,
        target_id: str,
        mappings: List[FieldMapping]
    ) -> None:
        """Publish mapping generation event to message bus."""
        event_payload = {
            "source_id": source_id,
            "target_id": target_id,
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
        
        message = Message(
            event_type="schema.mapping.generated",
            payload=event_payload,
            timestamp=datetime.now(),
            source=self.agent_id
        )
        
        self.message_bus.publish("mapping.events", message)
        logger.debug(f"Published mapping event for {source_id} -> {target_id}")
    
    def _publish_mapping_update_event(
        self,
        source_id: str,
        target_id: str,
        schema_changes: List[SchemaChange],
        updated_mappings: List[FieldMapping]
    ) -> None:
        """Publish mapping update event to message bus."""
        event_payload = {
            "source_id": source_id,
            "target_id": target_id,
            "change_count": len(schema_changes),
            "mapping_count": len(updated_mappings),
            "changes": [
                {
                    "change_type": c.change_type,
                    "field_name": c.field_name
                }
                for c in schema_changes
            ]
        }
        
        message = Message(
            event_type="schema.mapping.updated",
            payload=event_payload,
            timestamp=datetime.now(),
            source=self.agent_id
        )
        
        self.message_bus.publish("mapping.events", message)
        logger.debug(f"Published mapping update event for {source_id} -> {target_id}")
