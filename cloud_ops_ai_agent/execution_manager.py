"""Core execution management for the cloud-ops-ai-agent framework.

This module implements the ExecutionManager class, which serves as the central
orchestrator for cloud operations. It enforces a two-phase safety model inspired
by automated optimization workflows in large-scale mobile-cloud platforms:

  1. **High-Risk Gate** — operations are classified against a config-driven
     allowlist before any side-effecting code runs.
  2. **Confirmation Workflow** — flagged operations pause execution and require
     explicit operator acknowledgement, preventing accidental destructive actions.
  3. **Task Termination** — in-flight tasks expose a cancellation channel so that
     long-running jobs can be cleanly aborted at the next safe checkpoint.

Typical usage example::

    manager = ExecutionManager(config_path="config.json")
    manager.execute_task(
        task_id="task-001",
        operation="delete_instance",
        resource="prod-vm-42",
        action=lambda: cloud_client.delete("prod-vm-42"),
    )
"""

from __future__ import annotations

import json
import logging
import random
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from pathlib import Path
from typing import Any

from cloud_ops_ai_agent.exceptions import (
    CloudOpsBaseError,
    ConfigLoadError,
    ConfirmationRejectedError,
    ConfirmationTimeoutError,
    TaskTerminatedError,
    TaskTimeoutError,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Data types
# ---------------------------------------------------------------------------


class TaskStatus(Enum):
    """Lifecycle states of a managed task."""

    PENDING = auto()
    AWAITING_CONFIRMATION = auto()
    RUNNING = auto()
    COMPLETED = auto()
    TERMINATED = auto()
    FAILED = auto()


@dataclass
class TaskRecord:
    """Mutable runtime record for a managed task.

    Attributes:
        task_id: Globally unique identifier assigned at submission time.
        operation: Name of the cloud operation being executed.
        resource: Target cloud resource (instance ID, bucket name, etc.).
        status: Current lifecycle state of the task.
        created_at: Unix timestamp when the task was created.
        updated_at: Unix timestamp of the most recent status transition.
        error: Exception captured on failure, or ``None`` on success.
        result: Return value of the task callable on success.
    """

    task_id: str
    operation: str
    resource: str
    status: TaskStatus = TaskStatus.PENDING
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    error: Exception | None = None
    result: Any = None

    def transition(self, new_status: TaskStatus) -> None:
        """Atomically update the task status and refresh ``updated_at``.

        Args:
            new_status: The target ``TaskStatus`` to transition into.
        """
        self.status = new_status
        self.updated_at = time.time()


# ---------------------------------------------------------------------------
# Configuration schema
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ManagerConfig:
    """Immutable typed representation of values loaded from ``config.json``.

    Attributes:
        high_risk_operations: Frozen set of operation names that require confirmation.
        confirmation_timeout_seconds: Seconds to wait for operator input.
        confirm_token: Exact string the operator must type to proceed.
        prompt_template: f-string template rendered for confirmation prompts.
        max_retries: Maximum retry attempts for transient failures.
        retry_delay_seconds: Base back-off delay between consecutive retries.
        task_timeout_seconds: Hard wall-clock deadline for any single task.
    """

    high_risk_operations: frozenset[str]
    confirmation_timeout_seconds: int
    confirm_token: str
    prompt_template: str
    max_retries: int
    retry_delay_seconds: float
    task_timeout_seconds: float

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> ManagerConfig:
        """Construct a ``ManagerConfig`` from a raw JSON dictionary.

        Args:
            raw: The top-level dictionary parsed from ``config.json``.

        Returns:
            A fully validated ``ManagerConfig`` instance.

        Raises:
            KeyError: If a required configuration key is missing.
        """
        confirmation = raw["confirmation"]
        execution = raw["execution"]
        return cls(
            high_risk_operations=frozenset(raw["high_risk_operations"]),
            confirmation_timeout_seconds=int(
                confirmation["timeout_seconds"]
            ),
            confirm_token=confirmation["confirm_token"],
            prompt_template=confirmation["prompt_template"],
            max_retries=int(execution["max_retries"]),
            retry_delay_seconds=float(execution["retry_delay_seconds"]),
            task_timeout_seconds=float(execution["task_timeout_seconds"]),
        )


# ---------------------------------------------------------------------------
# ExecutionManager
# ---------------------------------------------------------------------------


class ExecutionManager:
    """Orchestrates cloud operations with safety gates and lifecycle control.

    The manager wraps arbitrary callables ("actions") in a structured pipeline:

      * Config-driven risk classification (``is_high_risk_operation``).
      * Human-in-the-loop confirmation for high-risk ops (``request_confirmation``).
      * Cooperative task cancellation via per-task ``threading.Event`` flags.
      * An in-memory task registry for audit and status queries.

    The design draws from production automation patterns where bulk operations
    (e.g., batch commodity optimisation, feature flag rollouts) must be gated
    against accidental destructive changes — particularly in environments shared
    across multiple product lines.

    Attributes:
        config: Resolved ``ManagerConfig`` loaded from the external JSON file.

    Example::

        manager = ExecutionManager("config.json")
        try:
            result = manager.execute_task(
                task_id="cleanup-001",
                operation="delete_instance",
                resource="stale-vm-7",
                action=lambda: True,
            )
        except ConfirmationRejectedError:
            print("Operator declined. No changes were made.")
    """

    def __init__(self, config_path: str = "config.json") -> None:
        """Initialise the manager and load operational configuration.

        Args:
            config_path: Path to the JSON configuration file that defines
                high-risk operations and workflow parameters. Defaults to
                ``"config.json"`` relative to the current working directory.

        Raises:
            ConfigLoadError: If the file cannot be opened or parsed as JSON,
                or if required keys are absent.
        """
        self.config: ManagerConfig = self._load_config(config_path)

        # task_id -> TaskRecord; guarded by _registry_lock for thread safety.
        self._task_registry: dict[str, TaskRecord] = {}
        self._registry_lock = threading.Lock()

        # task_id -> threading.Event; set to signal cancellation.
        self._cancel_flags: dict[str, threading.Event] = {}

        logger.info(
            "ExecutionManager initialised. High-risk operations: %s",
            self.config.high_risk_operations,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def execute_task(
        self,
        operation: str,
        resource: str,
        action: Callable[[], Any],
        *,
        task_id: str | None = None,
    ) -> Any:
        """Submit and run a cloud operation through the full safety pipeline.

        The execution flow is:

          1. Register the task and allocate a cancellation flag.
          2. If ``operation`` is high-risk, pause and request confirmation.
          3. Check the cancellation flag before starting the action.
          4. Invoke ``action`` with retry logic for transient failures.
          5. Record the final status (COMPLETED / TERMINATED / FAILED).

        Args:
            operation: Logical name of the cloud operation, e.g.
                ``"delete_instance"``. Must match keys in ``config.json`` if
                it is a high-risk operation.
            resource: Human-readable identifier of the target resource, used
                in confirmation prompts and audit records.
            action: Zero-argument callable that performs the actual cloud API
                call. Its return value is captured in the ``TaskRecord``.
            task_id: Optional caller-supplied identifier. A UUID is generated
                when omitted.

        Returns:
            The return value of ``action`` on success.

        Raises:
            TaskTerminatedError: If ``terminate_task`` was called while the
                task was awaiting confirmation or between retries.
            ConfirmationTimeoutError: If operator confirmation was not received
                within ``config.confirmation_timeout_seconds``.
            ConfirmationRejectedError: If the operator explicitly declined.
            CloudOpsBaseError: For any other managed failure within the pipeline.
        """
        resolved_id = task_id or str(uuid.uuid4())
        record = self._register_task(resolved_id, operation, resource)

        try:
            if self.is_high_risk_operation(operation):
                logger.warning(
                    "High-risk operation detected: '%s' on '%s' (task=%s)",
                    operation,
                    resource,
                    resolved_id,
                )
                record.transition(TaskStatus.AWAITING_CONFIRMATION)
                self.request_confirmation(
                    operation=operation,
                    resource=resource,
                    task_id=resolved_id,
                )

            self._check_cancellation(resolved_id)

            record.transition(TaskStatus.RUNNING)
            result = self._execute_with_retry(resolved_id, action)

            record.result = result
            record.transition(TaskStatus.COMPLETED)
            logger.info("Task '%s' completed successfully.", resolved_id)
            return result

        except TaskTerminatedError:
            record.transition(TaskStatus.TERMINATED)
            logger.info("Task '%s' was terminated.", resolved_id)
            raise
        except (ConfirmationTimeoutError, ConfirmationRejectedError):
            record.transition(TaskStatus.FAILED)
            raise
        except Exception as exc:  # pylint: disable=broad-except
            record.error = exc
            record.transition(TaskStatus.FAILED)
            logger.exception("Task '%s' failed unexpectedly.", resolved_id)
            raise
        finally:
            with self._registry_lock:
                self._cancel_flags.pop(resolved_id, None)

    def terminate_task(self, task_id: str) -> bool:
        """Signal a running or pending task to stop at its next checkpoint.

        Termination is *cooperative*: the flag is set here, and the task
        checks it before each significant step (pre-action check, between
        retries). This avoids forcibly killing threads, which can leave cloud
        resources in an inconsistent state.

        Args:
            task_id: The identifier of the task to terminate.

        Returns:
            ``True`` if the flag was set (task existed and was not already
            finished), ``False`` if the task is unknown or already in a
            terminal state.
        """
        with self._registry_lock:
            record = self._task_registry.get(task_id)
            if record is None:
                logger.warning(
                    "terminate_task called for unknown task '%s'.", task_id
                )
                return False

            terminal_states = {
                TaskStatus.COMPLETED,
                TaskStatus.TERMINATED,
                TaskStatus.FAILED,
            }
            if record.status in terminal_states:
                logger.info(
                    "Task '%s' is already in terminal state '%s'; "
                    "termination signal ignored.",
                    task_id,
                    record.status.name,
                )
                return False

            cancel_event = self._cancel_flags.get(task_id)
            if cancel_event:
                cancel_event.set()
                logger.info(
                    "Termination signal sent to task '%s'.", task_id
                )
            return True

    def request_confirmation(
        self,
        operation: str,
        resource: str,
        task_id: str,
    ) -> None:
        """Pause execution and request explicit operator confirmation.

        Renders a prompt using the template from ``config.json`` and reads
        user input from ``stdin``. Input collection runs in a daemon thread
        so that the main flow can enforce the timeout deadline via
        ``threading.Event``.

        This pattern mirrors the "二次确认" (double-confirmation) guard
        used in automated commodity optimisation pipelines, where a
        mis-classified item could trigger large-scale price changes.

        Args:
            operation: The high-risk operation name, embedded in the prompt.
            resource: The target resource identifier, embedded in the prompt.
            task_id: The task identifier, checked against cancellation flags.

        Raises:
            TaskTerminatedError: If the task is cancelled while waiting.
            ConfirmationTimeoutError: If no valid input arrives in time.
            ConfirmationRejectedError: If the operator types anything other
                than the configured confirmation token.
        """
        timeout = self.config.confirmation_timeout_seconds
        confirm_token = self.config.confirm_token
        prompt = self.config.prompt_template.format(
            operation=operation,
            resource=resource,
            confirm_token=confirm_token,
        )

        user_input_container: list[str] = []
        input_ready = threading.Event()

        def _read_input() -> None:
            try:
                response = input(prompt)
                user_input_container.append(response.strip())
            except EOFError:
                user_input_container.append("")
            finally:
                input_ready.set()

        input_thread = threading.Thread(target=_read_input, daemon=True)
        input_thread.start()

        # Poll in short intervals so we can also respect cancellation.
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if input_ready.wait(timeout=1.0):
                break
            self._check_cancellation(task_id)
        else:
            raise ConfirmationTimeoutError(
                operation=operation,
                timeout_seconds=timeout,
            )

        user_response = user_input_container[0] if user_input_container else ""

        if user_response != confirm_token:
            raise ConfirmationRejectedError(
                message=(
                    f"Operator declined operation '{operation}' on "
                    f"'{resource}'. Expected '{confirm_token}', "
                    f"got '{user_response}'."
                ),
                operation=operation,
            )

        logger.info(
            "Operator confirmed operation '%s' on '%s' (task=%s).",
            operation,
            resource,
            task_id,
        )

    def is_high_risk_operation(self, operation: str) -> bool:
        """Check whether an operation requires confirmation before execution.

        The list of high-risk operations is loaded from ``config.json``,
        making it straightforward to promote or demote an operation without
        touching application code — analogous to the 『功能说明配置化』
        (feature-configuration externalisation) pattern from production
        mobile-cloud feature management.

        Args:
            operation: The operation name to classify.

        Returns:
            ``True`` if the operation is in the configured high-risk set,
            ``False`` otherwise.

        Example::

            manager = ExecutionManager()
            if manager.is_high_risk_operation("delete_instance"):
                print("Confirmation required.")
        """
        return operation in self.config.high_risk_operations

    def get_task_record(self, task_id: str) -> TaskRecord | None:
        """Retrieve the runtime record for a submitted task.

        Args:
            task_id: The task identifier returned by (or passed to)
                ``execute_task``.

        Returns:
            The ``TaskRecord`` if the task is known, or ``None``.
        """
        with self._registry_lock:
            return self._task_registry.get(task_id)

    def list_task_records(self) -> list[TaskRecord]:
        """Return a snapshot of all submitted task records.

        Returns:
            A list of ``TaskRecord`` instances in arbitrary order.
        """
        with self._registry_lock:
            return list(self._task_registry.values())

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _load_config(self, config_path: str) -> ManagerConfig:
        """Read and parse the JSON configuration file.

        Args:
            config_path: Filesystem path to ``config.json``.

        Returns:
            A ``ManagerConfig`` populated from the file contents.

        Raises:
            ConfigLoadError: On I/O errors, JSON decode failures, or missing
                required keys.
        """
        path = Path(config_path)
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            config = ManagerConfig.from_dict(raw)
        except FileNotFoundError as exc:
            raise ConfigLoadError(
                config_path=config_path,
                reason=f"File not found: {exc}",
            ) from exc
        except json.JSONDecodeError as exc:
            raise ConfigLoadError(
                config_path=config_path,
                reason=f"Invalid JSON: {exc}",
            ) from exc
        except KeyError as exc:
            raise ConfigLoadError(
                config_path=config_path,
                reason=f"Missing required key: {exc}",
            ) from exc

        logger.debug("Configuration loaded from '%s'.", config_path)
        return config

    def _register_task(
        self,
        task_id: str,
        operation: str,
        resource: str,
    ) -> TaskRecord:
        """Create and store a new TaskRecord with its cancellation flag.

        Args:
            task_id: Unique identifier for the task.
            operation: Operation name.
            resource: Target resource identifier.

        Returns:
            The newly created ``TaskRecord``.
        """
        record = TaskRecord(
            task_id=task_id,
            operation=operation,
            resource=resource,
        )
        cancel_event = threading.Event()

        with self._registry_lock:
            self._task_registry[task_id] = record
            self._cancel_flags[task_id] = cancel_event

        logger.debug(
            "Registered task '%s': operation='%s', resource='%s'.",
            task_id,
            operation,
            resource,
        )
        return record

    def _check_cancellation(self, task_id: str) -> None:
        """Raise ``TaskTerminatedError`` if the task's cancel flag is set.

        This is a lightweight checkpoint that should be called at every
        safe point in the execution pipeline (between retries, before the
        primary action, while waiting for confirmation input).

        Args:
            task_id: The task whose flag to inspect.

        Raises:
            TaskTerminatedError: If the cancellation flag is set.
        """
        cancel_event = self._cancel_flags.get(task_id)
        if cancel_event and cancel_event.is_set():
            raise TaskTerminatedError(task_id=task_id)

    def _execute_with_retry(
        self,
        task_id: str,
        action: Callable[[], Any],
    ) -> Any:
        """Invoke ``action`` with exponential back-off retry logic.

        Transient cloud API failures (rate limits, network blips) are
        retried up to ``config.max_retries`` times.  Between each attempt
        the cancellation flag and the wall-clock deadline are checked so
        that a retry loop does not silently mask a termination request or
        run past the configured timeout.

        Retry delay formula::

            delay = base * 2^(attempt-1) * (1 + 0.1 * rand)

        The ±10 % jitter prevents correlated retries across concurrent
        tasks (thundering-herd mitigation).

        Args:
            task_id: The owning task's identifier (used for cancellation
                checks and structured log context).
            action: Zero-argument callable to invoke.

        Returns:
            The return value of ``action`` on the first successful call.

        Raises:
            TaskTimeoutError: If the task exceeds ``config.task_timeout_seconds``.
            TaskTerminatedError: If the task is cancelled between retries.
            Exception: The last exception raised by ``action`` after all
                retry attempts are exhausted.
        """
        timeout = self.config.task_timeout_seconds
        deadline = time.monotonic() + timeout
        last_exception: Exception | None = None
        max_attempts = self.config.max_retries + 1

        for attempt in range(1, max_attempts + 1):
            if time.monotonic() >= deadline:
                raise TaskTimeoutError(task_id=task_id, timeout_seconds=timeout)
            self._check_cancellation(task_id)
            try:
                logger.debug(
                    "Task '%s': attempt %d/%d.", task_id, attempt, max_attempts
                )
                return action()
            except CloudOpsBaseError:
                # Managed errors are not retried; propagate immediately.
                raise
            except Exception as exc:  # pylint: disable=broad-except
                last_exception = exc
                logger.warning(
                    "Task '%s' attempt %d failed: %s",
                    task_id,
                    attempt,
                    exc,
                )
                if attempt < max_attempts:
                    base_delay = self.config.retry_delay_seconds * (2 ** (attempt - 1))
                    jitter = base_delay * 0.1 * random.random()
                    delay = min(base_delay + jitter, max(0.0, deadline - time.monotonic()))
                    logger.info(
                        "Retrying task '%s' in %.2fs…", task_id, delay
                    )
                    time.sleep(delay)

        raise last_exception  # type: ignore[misc]
