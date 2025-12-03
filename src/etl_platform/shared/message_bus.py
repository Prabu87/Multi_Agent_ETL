"""Message bus infrastructure for inter-component communication."""

from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime
import json


@dataclass
class Message:
    """Base message class for event communication."""
    
    event_type: str
    payload: Dict[str, Any]
    timestamp: datetime
    source: str
    correlation_id: Optional[str] = None
    
    def to_json(self) -> str:
        """Serialize message to JSON."""
        data = {
            "event_type": self.event_type,
            "payload": self.payload,
            "timestamp": self.timestamp.isoformat(),
            "source": self.source,
            "correlation_id": self.correlation_id,
        }
        return json.dumps(data)
    
    @classmethod
    def from_json(cls, json_str: str) -> "Message":
        """Deserialize message from JSON."""
        data = json.loads(json_str)
        data["timestamp"] = datetime.fromisoformat(data["timestamp"])
        return cls(**data)


class MessageBus(ABC):
    """Abstract base class for message bus implementations."""
    
    @abstractmethod
    def publish(self, topic: str, message: Message) -> None:
        """Publish a message to a topic."""
        pass
    
    @abstractmethod
    def subscribe(self, topic: str, handler: Callable[[Message], None]) -> None:
        """Subscribe to a topic with a message handler."""
        pass
    
    @abstractmethod
    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a topic."""
        pass
    
    @abstractmethod
    def close(self) -> None:
        """Close the message bus connection."""
        pass


class InMemoryMessageBus(MessageBus):
    """In-memory message bus implementation for testing and development."""
    
    def __init__(self) -> None:
        self._subscribers: Dict[str, List[Callable[[Message], None]]] = {}
    
    def publish(self, topic: str, message: Message) -> None:
        """Publish a message to all subscribers of a topic."""
        if topic in self._subscribers:
            for handler in self._subscribers[topic]:
                handler(message)
    
    def subscribe(self, topic: str, handler: Callable[[Message], None]) -> None:
        """Subscribe to a topic."""
        if topic not in self._subscribers:
            self._subscribers[topic] = []
        self._subscribers[topic].append(handler)
    
    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a topic."""
        if topic in self._subscribers:
            del self._subscribers[topic]
    
    def close(self) -> None:
        """Close the message bus (no-op for in-memory)."""
        self._subscribers.clear()


class RedisMessageBus(MessageBus):
    """Redis-based message bus implementation using pub/sub."""
    
    def __init__(self, redis_url: str = "redis://localhost:6379") -> None:
        try:
            import redis
            self._redis = redis.from_url(redis_url)
            self._pubsub = self._redis.pubsub()
            self._handlers: Dict[str, Callable[[Message], None]] = {}
        except ImportError:
            raise ImportError("redis package is required for RedisMessageBus")
    
    def publish(self, topic: str, message: Message) -> None:
        """Publish a message to a Redis channel."""
        self._redis.publish(topic, message.to_json())
    
    def subscribe(self, topic: str, handler: Callable[[Message], None]) -> None:
        """Subscribe to a Redis channel."""
        self._handlers[topic] = handler
        self._pubsub.subscribe(**{topic: self._message_handler})
    
    def _message_handler(self, message: Dict[str, Any]) -> None:
        """Internal handler for Redis messages."""
        if message["type"] == "message":
            channel = message["channel"].decode("utf-8")
            data = message["data"].decode("utf-8")
            msg = Message.from_json(data)
            if channel in self._handlers:
                self._handlers[channel](msg)
    
    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a Redis channel."""
        self._pubsub.unsubscribe(topic)
        if topic in self._handlers:
            del self._handlers[topic]
    
    def close(self) -> None:
        """Close the Redis connection."""
        self._pubsub.close()
        self._redis.close()


class KafkaMessageBus(MessageBus):
    """Kafka-based message bus implementation."""
    
    def __init__(self, bootstrap_servers: str = "localhost:9092") -> None:
        try:
            from kafka import KafkaProducer, KafkaConsumer
            self._producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda v: v.encode("utf-8")
            )
            self._consumers: Dict[str, KafkaConsumer] = {}
            self._bootstrap_servers = bootstrap_servers
        except ImportError:
            raise ImportError("kafka-python package is required for KafkaMessageBus")
    
    def publish(self, topic: str, message: Message) -> None:
        """Publish a message to a Kafka topic."""
        self._producer.send(topic, message.to_json())
        self._producer.flush()
    
    def subscribe(self, topic: str, handler: Callable[[Message], None]) -> None:
        """Subscribe to a Kafka topic."""
        from kafka import KafkaConsumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self._bootstrap_servers,
            value_deserializer=lambda v: v.decode("utf-8")
        )
        self._consumers[topic] = consumer
        
        # Start consuming in a separate thread
        import threading
        def consume() -> None:
            for record in consumer:
                msg = Message.from_json(record.value)
                handler(msg)
        
        thread = threading.Thread(target=consume, daemon=True)
        thread.start()
    
    def unsubscribe(self, topic: str) -> None:
        """Unsubscribe from a Kafka topic."""
        if topic in self._consumers:
            self._consumers[topic].close()
            del self._consumers[topic]
    
    def close(self) -> None:
        """Close all Kafka connections."""
        self._producer.close()
        for consumer in self._consumers.values():
            consumer.close()
        self._consumers.clear()
