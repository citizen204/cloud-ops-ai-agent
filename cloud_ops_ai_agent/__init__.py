"""cloud-ops-ai-agent: AI-driven cloud operations management framework.

This package provides the core primitives for safe, auditable execution of
cloud operations, including risk classification, confirmation workflows, and
cooperative task cancellation.
"""

from cloud_ops_ai_agent.exceptions import (
    CloudOpsBaseError,
    ConfigLoadError,
    ConfirmationRejectedError,
    ConfirmationTimeoutError,
    TaskTerminatedError,
    TaskTimeoutError,
)
from cloud_ops_ai_agent.execution_manager import ExecutionManager, TaskStatus

__all__ = [
    "ExecutionManager",
    "TaskStatus",
    "CloudOpsBaseError",
    "ConfigLoadError",
    "ConfirmationRejectedError",
    "ConfirmationTimeoutError",
    "TaskTerminatedError",
    "TaskTimeoutError",
]
