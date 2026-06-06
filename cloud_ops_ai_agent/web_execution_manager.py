"""HTTP-aware execution manager for REST API deployment.

The base ``ExecutionManager`` uses ``input()`` to block on confirmation, which
is incompatible with a web server environment.  ``WebExecutionManager`` replaces
that blocking stdin read with a ``threading.Event``-based mechanism:

  1. ``submit_task_async`` registers the task, starts it in a background thread,
     and returns the ``task_id`` immediately — the HTTP handler can respond
     without waiting for the job to complete.
  2. When the task reaches a high-risk gate it sets its status to
     ``AWAITING_CONFIRMATION`` and blocks on a per-task ``threading.Event``.
  3. The caller hits ``POST /api/v1/tasks/{task_id}/confirm`` which invokes
     ``confirm_task(task_id, confirmed=True/False)``, setting the event and
     unblocking the worker thread.

This design keeps all the safety semantics of the base class intact while
adapting the confirmation I/O layer to an async HTTP context.
"""

from __future__ import annotations

import concurrent.futures
import logging
import threading
import time
from collections.abc import Callable
from typing import Any

from cloud_ops_ai_agent.exceptions import (
    ConfirmationRejectedError,
    ConfirmationTimeoutError,
    TaskTerminatedError,
)
from cloud_ops_ai_agent.execution_manager import ExecutionManager

logger = logging.getLogger(__name__)


class WebExecutionManager(ExecutionManager):
    """``ExecutionManager`` variant designed for HTTP API deployment.

    Overrides the stdin-based confirmation workflow with a non-blocking,
    event-driven mechanism suitable for concurrent REST API usage.

    Tasks are dispatched into a thread pool so the HTTP layer never blocks
    waiting for a long-running cloud operation.

    Attributes:
        config: Resolved ``ManagerConfig`` (inherited).

    Example::

        manager = WebExecutionManager("config.json")

        # Returns task_id immediately; task runs in background.
        task_id = manager.submit_task_async(
            operation="delete_instance",
            resource="prod-vm-42",
            action=lambda: True,
        )

        # Later, from a separate HTTP request:
        manager.confirm_task(task_id, confirmed=True)
    """

    _MAX_WORKERS = 16

    def __init__(self, config_path: str = "config.json") -> None:
        super().__init__(config_path)

        # task_id -> threading.Event  (set when a confirmation decision arrives)
        self._confirmation_events: dict[str, threading.Event] = {}

        # task_id -> bool  (True = confirmed, False = rejected)
        self._confirmation_decisions: dict[str, bool] = {}

        self._conf_lock = threading.Lock()
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._MAX_WORKERS,
            thread_name_prefix="cloud-ops-worker",
        )

    # ------------------------------------------------------------------
    # Public API (additions to base class)
    # ------------------------------------------------------------------

    def submit_task_async(
        self,
        operation: str,
        resource: str,
        action: Callable[[], Any],
        *,
        task_id: str | None = None,
    ) -> str:
        """Dispatch a task to the thread pool and return its ID immediately.

        Unlike the synchronous ``execute_task``, this method does not block.
        The caller receives a ``task_id`` that can be used to poll status,
        confirm high-risk operations, or request termination.

        Args:
            operation: Logical cloud operation name.
            resource: Target resource identifier.
            action: Zero-argument callable that performs the operation.
            task_id: Optional caller-supplied identifier; a UUID is generated
                when omitted.

        Returns:
            The resolved ``task_id`` string.
        """
        import uuid as _uuid

        resolved_id = task_id or str(_uuid.uuid4())

        # Pre-register the confirmation event before the thread starts so
        # that a fast-path ``confirm_task`` call cannot arrive before the
        # event exists.
        with self._conf_lock:
            self._confirmation_events[resolved_id] = threading.Event()

        self._executor.submit(self._run_task, resolved_id, operation, resource, action)
        logger.info("Task '%s' submitted asynchronously.", resolved_id)
        return resolved_id

    def confirm_task(self, task_id: str, *, confirmed: bool) -> bool:
        """Deliver an operator decision to a task awaiting confirmation.

        Args:
            task_id: The task identifier returned by ``submit_task_async``.
            confirmed: ``True`` to approve the high-risk operation, ``False``
                to reject it.

        Returns:
            ``True`` if the signal was delivered, ``False`` if the task is
            unknown (already finished or never submitted).
        """
        with self._conf_lock:
            event = self._confirmation_events.get(task_id)
            if event is None:
                logger.warning(
                    "confirm_task: no pending confirmation for task '%s'.",
                    task_id,
                )
                return False
            self._confirmation_decisions[task_id] = confirmed
            event.set()

        action_label = "confirmed" if confirmed else "rejected"
        logger.info(
            "Task '%s' confirmation %s by operator.", task_id, action_label
        )
        return True

    def shutdown(self, wait: bool = True) -> None:
        """Gracefully shut down the worker thread pool.

        Args:
            wait: If ``True``, block until all submitted tasks complete.
        """
        self._executor.shutdown(wait=wait)

    # ------------------------------------------------------------------
    # Override: confirmation via threading.Event instead of stdin
    # ------------------------------------------------------------------

    def request_confirmation(
        self,
        operation: str,
        resource: str,
        task_id: str,
    ) -> None:
        """Block the worker thread until the HTTP layer delivers a decision.

        This overrides the base-class ``input()`` call with an event wait so
        that the worker thread sleeps without holding any GIL-heavy resources.
        The main thread (and all other workers) remain fully unblocked.

        Args:
            operation: The high-risk operation name.
            resource: The target resource identifier.
            task_id: The task identifier, checked for cancellation.

        Raises:
            TaskTerminatedError: If the task is cancelled while waiting.
            ConfirmationTimeoutError: If no decision arrives within the window.
            ConfirmationRejectedError: If the operator rejected the operation.
        """
        timeout = self.config.confirmation_timeout_seconds
        with self._conf_lock:
            event = self._confirmation_events.get(task_id)

        if event is None:
            raise ConfirmationTimeoutError(
                operation=operation, timeout_seconds=timeout
            )

        logger.info(
            "Task '%s' is AWAITING_CONFIRMATION for operation '%s' on '%s'. "
            "Timeout: %ds.",
            task_id,
            operation,
            resource,
            timeout,
        )

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if event.wait(timeout=1.0):
                break
            self._check_cancellation(task_id)
        else:
            raise ConfirmationTimeoutError(
                operation=operation,
                timeout_seconds=timeout,
            )

        with self._conf_lock:
            confirmed = self._confirmation_decisions.get(task_id, False)

        if not confirmed:
            raise ConfirmationRejectedError(
                message=(
                    f"Operator rejected operation '{operation}' on "
                    f"resource '{resource}'."
                ),
                operation=operation,
            )

        logger.info(
            "Task '%s': operator confirmed '%s' on '%s'.",
            task_id,
            operation,
            resource,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_task(
        self,
        task_id: str,
        operation: str,
        resource: str,
        action: Callable[[], Any],
    ) -> None:
        """Worker entry point — wraps ``execute_task`` for thread-pool use.

        All exceptions are absorbed here; callers poll status via the task
        registry rather than catching exceptions from the future.

        Args:
            task_id: Pre-registered task identifier.
            operation: Cloud operation name.
            resource: Target resource identifier.
            action: The callable to invoke.
        """
        try:
            self.execute_task(
                operation=operation,
                resource=resource,
                action=action,
                task_id=task_id,
            )
        except (TaskTerminatedError, ConfirmationRejectedError, ConfirmationTimeoutError):
            pass  # Captured in TaskRecord; not unexpected.
        except Exception:
            logger.exception("Unhandled error in task '%s'.", task_id)
        finally:
            with self._conf_lock:
                self._confirmation_events.pop(task_id, None)
                self._confirmation_decisions.pop(task_id, None)
