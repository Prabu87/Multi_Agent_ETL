"""Base agent class using LangChain and LangGraph."""

import uuid
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, TypedDict
from datetime import datetime
import logging

from langchain_core.messages import BaseMessage, HumanMessage, AIMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import StateGraph, END

from etl_platform.shared.message_bus import Message, MessageBus


logger = logging.getLogger(__name__)


class AgentState(TypedDict):
    """Base state for agent graph execution."""
    messages: List[BaseMessage]
    task_id: str
    context: Dict[str, Any]
    result: Optional[Dict[str, Any]]
    error: Optional[str]


class BaseAgent(ABC):
    """Base class for all agents using LangGraph architecture."""
    
    def __init__(
        self,
        message_bus: MessageBus,
        agent_id: Optional[str] = None,
        agent_type: str = "base"
    ):
        """
        Initialize the base agent.
        
        Args:
            message_bus: Message bus for publishing events
            agent_id: Unique identifier for this agent instance
            agent_type: Type of agent (for logging and identification)
        """
        self.message_bus = message_bus
        self.agent_id = agent_id or f"{agent_type}-{uuid.uuid4().hex[:8]}"
        self.agent_type = agent_type
        self.graph = self._build_graph()
        logger.info(f"{agent_type.title()} Agent initialized: {self.agent_id}")
    
    @abstractmethod
    def _build_graph(self) -> StateGraph:
        """
        Build the agent's execution graph.
        
        Returns:
            Compiled StateGraph for agent execution
        """
        pass
    
    def publish_event(
        self,
        event_type: str,
        payload: Dict[str, Any],
        topic: str
    ) -> None:
        """
        Publish an event to the message bus.
        
        Args:
            event_type: Type of event
            payload: Event payload
            topic: Topic to publish to
        """
        message = Message(
            event_type=event_type,
            payload=payload,
            timestamp=datetime.now(),
            source=self.agent_id
        )
        self.message_bus.publish(topic, message)
        logger.info(f"Published {event_type} event to {topic}")
    
    def execute(self, initial_state: AgentState) -> AgentState:
        """
        Execute the agent's graph with the given initial state.
        
        Args:
            initial_state: Initial state for execution
            
        Returns:
            Final state after execution
        """
        logger.info(f"Executing agent {self.agent_id} for task {initial_state.get('task_id')}")
        try:
            final_state = self.graph.invoke(initial_state)
            logger.info(f"Agent execution completed successfully")
            return final_state
        except Exception as e:
            logger.error(f"Agent execution failed: {str(e)}")
            raise
