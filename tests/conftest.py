"""Pytest configuration and fixtures."""

import pytest
from etl_platform.shared.message_bus import InMemoryMessageBus


@pytest.fixture
def message_bus():
    """Provide an in-memory message bus for testing."""
    bus = InMemoryMessageBus()
    yield bus
    bus.close()
