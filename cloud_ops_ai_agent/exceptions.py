"""Custom exceptions for the cloud-ops-ai-agent execution framework.

This module defines the exception hierarchy used throughout the project,
inspired by structured error handling patterns from large-scale cloud operations.
"""


class CloudOpsBaseError(Exception):
    """Base exception class for all cloud-ops-ai-agent errors.

    All custom exceptions in this project inherit from this class,
    enabling callers to catch the entire exception family with a single clause.

    Attributes:
        message: Human-readable description of the error.
        operation: The cloud operation that triggered the error, if applicable.
    """

    def __init__(self, message: str, operation: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.operation = operation

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"message={self.message!r}, operation={self.operation!r})"
        )


class TaskTerminatedError(CloudOpsBaseError):
    """Raised when a task is explicitly terminated before completion.

    This exception is thrown when an in-flight task receives a termination
    signal, either from a user request or a system-level watchdog.

    Attributes:
        task_id: The unique identifier of the terminated task.
    """

    def __init__(self, task_id: str, operation: str | None = None) -> None:
        message = f"Task '{task_id}' was terminated before completion."
        super().__init__(message, operation=operation)
        self.task_id = task_id


class ConfirmationTimeoutError(CloudOpsBaseError):
    """Raised when user confirmation is not received within the allowed window.

    For high-risk operations, a confirmation prompt is issued. If no valid
    input arrives before the deadline, this exception aborts the operation
    rather than proceeding with a potentially destructive action.

    Attributes:
        timeout_seconds: The duration (in seconds) that was allowed for input.
    """

    def __init__(
        self,
        operation: str,
        timeout_seconds: int,
    ) -> None:
        message = (
            f"Confirmation for operation '{operation}' timed out "
            f"after {timeout_seconds}s. Operation aborted."
        )
        super().__init__(message, operation=operation)
        self.timeout_seconds = timeout_seconds


class ConfirmationRejectedError(CloudOpsBaseError):
    """Raised when the operator explicitly declines to confirm a high-risk op.

    Distinct from a timeout — the user was present but chose not to proceed,
    which may indicate an erroneous or unintended request.
    """


class ConfigLoadError(CloudOpsBaseError):
    """Raised when the external config.json cannot be loaded or parsed.

    Attributes:
        config_path: Filesystem path of the config file that failed to load.
    """

    def __init__(self, config_path: str, reason: str) -> None:
        message = f"Failed to load config from '{config_path}': {reason}"
        super().__init__(message)
        self.config_path = config_path
        self.reason = reason


class TaskTimeoutError(CloudOpsBaseError):
    """Raised when a task exceeds its configured wall-clock deadline.

    Attributes:
        task_id: The identifier of the timed-out task.
        timeout_seconds: The deadline that was exceeded.
    """

    def __init__(self, task_id: str, timeout_seconds: float) -> None:
        message = (
            f"Task '{task_id}' exceeded timeout of {timeout_seconds}s."
        )
        super().__init__(message)
        self.task_id = task_id
        self.timeout_seconds = timeout_seconds
