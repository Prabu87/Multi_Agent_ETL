"""Tests for message bus infrastructure."""

import pytest
from datetime import datetime
from etl_platform.shared.message_bus import Message, InMemoryMessageBus


def test_message_serialization():
    """Test message serialization and deserialization."""
    msg = Message(
        event_type="test.event",
        payload={"key": "value"},
        timestamp=datetime(2024, 1, 1, 12, 0, 0),
        source="test_source",
        correlation_id="test-123"
    )
    
    json_str = msg.to_json()
    restored = Message.from_json(json_str)
    
    assert restored.event_type == msg.event_type
    assert restored.payload == msg.payload
    assert restored.timestamp == msg.timestamp
    assert restored.source == msg.source
    assert restored.correlation_id == msg.correlation_id


def test_in_memory_message_bus_publish_subscribe(message_bus):
    """Test publishing and subscribing to messages."""
    received_messages = []
    
    def handler(msg: Message) -> None:
        received_messages.append(msg)
    
    message_bus.subscribe("test.topic", handler)
    
    msg = Message(
        event_type="test.event",
        payload={"data": "test"},
        timestamp=datetime.now(),
        source="test"
    )
    
    message_bus.publish("test.topic", msg)
    
    assert len(received_messages) == 1
    assert received_messages[0].event_type == "test.event"
    assert received_messages[0].payload == {"data": "test"}


def test_in_memory_message_bus_multiple_subscribers(message_bus):
    """Test multiple subscribers receive the same message."""
    received_1 = []
    received_2 = []
    
    message_bus.subscribe("test.topic", lambda msg: received_1.append(msg))
    message_bus.subscribe("test.topic", lambda msg: received_2.append(msg))
    
    msg = Message(
        event_type="test.event",
        payload={"data": "test"},
        timestamp=datetime.now(),
        source="test"
    )
    
    message_bus.publish("test.topic", msg)
    
    assert len(received_1) == 1
    assert len(received_2) == 1


def test_in_memory_message_bus_unsubscribe(message_bus):
    """Test unsubscribing from a topic."""
    received_messages = []
    
    message_bus.subscribe("test.topic", lambda msg: received_messages.append(msg))
    message_bus.unsubscribe("test.topic")
    
    msg = Message(
        event_type="test.event",
        payload={"data": "test"},
        timestamp=datetime.now(),
        source="test"
    )
    
    message_bus.publish("test.topic", msg)
    
    assert len(received_messages) == 0
