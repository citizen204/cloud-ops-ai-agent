"""Pydantic request / response schemas for the cloud-ops-ai-agent REST API."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class SubmitTaskRequest(BaseModel):
    """Payload for ``POST /api/v1/tasks``.

    Attributes:
        operation: The logical cloud operation to execute (must match a key
            in ``OPERATION_REGISTRY``).
        resource: Human-readable target resource identifier used in
            confirmation prompts and audit logs.
        params: Optional key-value pairs forwarded as keyword arguments to the
            underlying operation function.
    """

    operation: str = Field(
        ...,
        examples=["delete_instance"],
        description="Operation name from the operation registry.",
    )
    resource: str = Field(
        ...,
        examples=["vm-prod-01"],
        description="Target resource identifier (for audit and confirmation prompts).",
    )
    params: dict[str, Any] = Field(
        default_factory=dict,
        examples=[{"instance_id": "vm-prod-01"}],
        description="Operation-specific keyword arguments.",
    )


class ConfirmTaskRequest(BaseModel):
    """Payload for ``POST /api/v1/tasks/{task_id}/confirm``.

    Attributes:
        confirmed: ``True`` to approve the pending high-risk operation,
            ``False`` to reject it.
    """

    confirmed: bool = Field(
        ...,
        description="Set to true to approve, false to reject the operation.",
    )


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class TaskResponse(BaseModel):
    """Unified task summary returned by most task endpoints.

    Attributes:
        task_id: Unique identifier of the task.
        operation: The requested operation name.
        resource: Target resource identifier.
        status: Current lifecycle state (e.g. ``"PENDING"``, ``"RUNNING"``).
        result: Return value of the action callable on successful completion.
        error: Error message on failure, or ``None``.
        created_at: Unix timestamp of task creation.
        updated_at: Unix timestamp of the last status transition.
    """

    task_id: str
    operation: str
    resource: str
    status: str
    result: Any = None
    error: str | None = None
    created_at: float
    updated_at: float


class SubmitTaskResponse(BaseModel):
    """Response to a successful task submission.

    Attributes:
        task_id: Use this ID to poll status, confirm, or terminate.
        status: Initial status (``"PENDING"``).
        is_high_risk: Whether a confirmation step will be required.
        message: Human-readable summary.
    """

    task_id: str
    status: str
    is_high_risk: bool
    message: str


class ConfirmResponse(BaseModel):
    """Response to a confirmation signal.

    Attributes:
        task_id: The task that received the signal.
        confirmed: The decision that was delivered.
        message: Human-readable outcome.
    """

    task_id: str
    confirmed: bool
    message: str


class TerminateResponse(BaseModel):
    """Response to a termination request.

    Attributes:
        task_id: The task targeted for termination.
        signalled: ``True`` if the cancellation flag was set.
        message: Human-readable outcome.
    """

    task_id: str
    signalled: bool
    message: str


class OperationInfo(BaseModel):
    """Metadata about a single registered operation.

    Attributes:
        name: Operation identifier.
        description: One-line summary of the operation.
        risk: ``"safe"`` or ``"high"``.
        params: List of required parameter names.
    """

    name: str
    description: str
    risk: str
    params: list[str]


class HealthResponse(BaseModel):
    """Response to ``GET /health``.

    Attributes:
        status: Always ``"ok"`` while the service is healthy.
        version: Application version string.
        high_risk_operations: Set of operations requiring confirmation.
    """

    status: str
    version: str
    high_risk_operations: list[str]
