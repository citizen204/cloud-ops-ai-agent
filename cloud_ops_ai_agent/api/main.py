"""FastAPI application entry point for cloud-ops-ai-agent.

Exposes the ExecutionManager's safety pipeline as a REST API so that
operators (or other AI agents) can submit, monitor, confirm, and terminate
cloud operations over HTTP.

API surface:

  GET  /health                            — liveness / readiness check
  GET  /api/v1/operations                 — list available operations
  POST /api/v1/tasks                      — submit a new task
  GET  /api/v1/tasks                      — list all tasks
  GET  /api/v1/tasks/{task_id}            — get task details
  POST /api/v1/tasks/{task_id}/confirm    — confirm / reject a high-risk task
  DELETE /api/v1/tasks/{task_id}          — terminate a running task
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Path
from fastapi.middleware.cors import CORSMiddleware

from cloud_ops_ai_agent.api.schemas import (
    ConfirmResponse,
    ConfirmTaskRequest,
    HealthResponse,
    OperationInfo,
    SubmitTaskRequest,
    SubmitTaskResponse,
    TaskResponse,
    TerminateResponse,
)
from cloud_ops_ai_agent.mock_operations import OPERATION_REGISTRY
from cloud_ops_ai_agent.web_execution_manager import WebExecutionManager

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

_APP_VERSION = "0.1.0"
_CONFIG_PATH = os.getenv("CONFIG_PATH", "config.json")
_CORS_ORIGINS: list[str] = os.getenv("CORS_ORIGINS", "").split(",") if os.getenv("CORS_ORIGINS") else ["*"]

# Module-level manager instance (initialised in lifespan).
_manager: WebExecutionManager | None = None


# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI):  # type: ignore[type-arg]
    """Initialise shared resources on startup; clean up on shutdown."""
    global _manager  # noqa: PLW0603
    _manager = WebExecutionManager(config_path=_CONFIG_PATH)
    logger.info("WebExecutionManager ready. Config: %s", _CONFIG_PATH)
    yield
    if _manager:
        _manager.shutdown(wait=False)
        logger.info("WebExecutionManager shut down.")


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    """Construct and configure the FastAPI application.

    Returns:
        A fully configured ``FastAPI`` instance ready to be served.
    """
    app = FastAPI(
        title="Cloud Ops AI Agent",
        description=(
            "AI-driven cloud operations management with built-in safety gates: "
            "high-risk classification, confirmation workflows, and cooperative "
            "task cancellation."
        ),
        version=_APP_VERSION,
        lifespan=lifespan,
        docs_url="/docs",
        redoc_url="/redoc",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=_CORS_ORIGINS,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- helpers ----

    def _get_manager() -> WebExecutionManager:
        if _manager is None:
            raise HTTPException(status_code=503, detail="Manager not initialised.")
        return _manager

    def _task_to_response(task_id: str) -> TaskResponse:
        mgr = _get_manager()
        record = mgr.get_task_record(task_id)
        if record is None:
            raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found.")
        return TaskResponse(
            task_id=record.task_id,
            operation=record.operation,
            resource=record.resource,
            status=record.status.name,
            result=record.result,
            error=str(record.error) if record.error else None,
            created_at=record.created_at,
            updated_at=record.updated_at,
        )

    # ----------------------------------------------------------------
    # Health
    # ----------------------------------------------------------------

    @app.get(
        "/health",
        response_model=HealthResponse,
        summary="Health check",
        tags=["System"],
    )
    def health_check() -> HealthResponse:
        """Return service liveness status and configuration summary."""
        mgr = _get_manager()
        return HealthResponse(
            status="ok",
            version=_APP_VERSION,
            high_risk_operations=sorted(mgr.config.high_risk_operations),
        )

    # ----------------------------------------------------------------
    # Operations registry
    # ----------------------------------------------------------------

    @app.get(
        "/api/v1/operations",
        response_model=list[OperationInfo],
        summary="List available operations",
        tags=["Operations"],
    )
    def list_operations() -> list[OperationInfo]:
        """Return all registered cloud operations with risk classification."""
        mgr = _get_manager()
        result = []
        for name, meta in OPERATION_REGISTRY.items():
            result.append(
                OperationInfo(
                    name=name,
                    description=meta["description"],
                    risk="high" if mgr.is_high_risk_operation(name) else "safe",
                    params=meta["params"],
                )
            )
        return result

    # ----------------------------------------------------------------
    # Task submission
    # ----------------------------------------------------------------

    @app.post(
        "/api/v1/tasks",
        response_model=SubmitTaskResponse,
        status_code=202,
        summary="Submit a cloud operation task",
        tags=["Tasks"],
    )
    def submit_task(body: SubmitTaskRequest) -> SubmitTaskResponse:
        """Submit a cloud operation for execution.

        If the operation is classified as **high-risk**, the task will pause in
        ``AWAITING_CONFIRMATION`` state.  Send a ``POST /api/v1/tasks/{task_id}/confirm``
        request to approve or reject the operation before it proceeds.

        Safe operations run immediately in the background.
        """
        mgr = _get_manager()

        op_meta = OPERATION_REGISTRY.get(body.operation)
        if op_meta is None:
            raise HTTPException(
                status_code=400,
                detail=(
                    f"Unknown operation '{body.operation}'. "
                    f"Available: {list(OPERATION_REGISTRY.keys())}"
                ),
            )

        required_params: list[str] = op_meta["params"]
        missing = [p for p in required_params if p not in body.params]
        if missing:
            raise HTTPException(
                status_code=422,
                detail=f"Missing required params for '{body.operation}': {missing}",
            )

        fn = op_meta["fn"]
        params: dict[str, Any] = body.params

        def action() -> Any:
            return fn(**params)

        task_id = mgr.submit_task_async(
            operation=body.operation,
            resource=body.resource,
            action=action,
        )

        is_high_risk = mgr.is_high_risk_operation(body.operation)
        if is_high_risk:
            msg = (
                f"Task '{task_id}' submitted. Operation '{body.operation}' is "
                f"HIGH RISK — send POST /api/v1/tasks/{task_id}/confirm with "
                f"{{\"confirmed\": true}} to proceed."
            )
        else:
            msg = f"Task '{task_id}' submitted and running in background."

        return SubmitTaskResponse(
            task_id=task_id,
            status="PENDING",
            is_high_risk=is_high_risk,
            message=msg,
        )

    # ----------------------------------------------------------------
    # Task listing
    # ----------------------------------------------------------------

    @app.get(
        "/api/v1/tasks",
        response_model=list[TaskResponse],
        summary="List all tasks",
        tags=["Tasks"],
    )
    def list_tasks() -> list[TaskResponse]:
        """Return a snapshot of all submitted tasks."""
        mgr = _get_manager()
        return [
            TaskResponse(
                task_id=record.task_id,
                operation=record.operation,
                resource=record.resource,
                status=record.status.name,
                result=record.result,
                error=str(record.error) if record.error else None,
                created_at=record.created_at,
                updated_at=record.updated_at,
            )
            for record in mgr.list_task_records()
        ]

    # ----------------------------------------------------------------
    # Task detail
    # ----------------------------------------------------------------

    @app.get(
        "/api/v1/tasks/{task_id}",
        response_model=TaskResponse,
        summary="Get task details",
        tags=["Tasks"],
    )
    def get_task(
        task_id: str = Path(..., description="Task identifier"),
    ) -> TaskResponse:
        """Return the current state and result of a specific task."""
        return _task_to_response(task_id)

    # ----------------------------------------------------------------
    # Confirmation
    # ----------------------------------------------------------------

    @app.post(
        "/api/v1/tasks/{task_id}/confirm",
        response_model=ConfirmResponse,
        summary="Confirm or reject a high-risk task",
        tags=["Tasks"],
    )
    def confirm_task(
        body: ConfirmTaskRequest,
        task_id: str = Path(..., description="Task identifier"),
    ) -> ConfirmResponse:
        """Deliver an operator decision to a task awaiting confirmation.

        Set ``confirmed`` to ``true`` to approve the high-risk operation, or
        ``false`` to reject it.  If the task has already completed, timed out,
        or was terminated, this endpoint returns HTTP 409.
        """
        mgr = _get_manager()
        delivered = mgr.confirm_task(task_id, confirmed=body.confirmed)
        if not delivered:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Task '{task_id}' is not awaiting confirmation "
                    "(it may not exist, have already completed, or timed out)."
                ),
            )
        action_word = "approved" if body.confirmed else "rejected"
        return ConfirmResponse(
            task_id=task_id,
            confirmed=body.confirmed,
            message=f"Operation {action_word}. Task will proceed accordingly.",
        )

    # ----------------------------------------------------------------
    # Termination
    # ----------------------------------------------------------------

    @app.delete(
        "/api/v1/tasks/{task_id}",
        response_model=TerminateResponse,
        summary="Terminate a running task",
        tags=["Tasks"],
    )
    def terminate_task(
        task_id: str = Path(..., description="Task identifier"),
    ) -> TerminateResponse:
        """Send a cooperative termination signal to a running or pending task.

        The task will stop at its next cancellation checkpoint.  Already-completed
        tasks return HTTP 409.
        """
        mgr = _get_manager()
        signalled = mgr.terminate_task(task_id)
        if not signalled:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Task '{task_id}' could not be terminated "
                    "(unknown, already finished, or in a terminal state)."
                ),
            )
        return TerminateResponse(
            task_id=task_id,
            signalled=True,
            message=f"Termination signal sent to task '{task_id}'.",
        )

    return app


app = create_app()
