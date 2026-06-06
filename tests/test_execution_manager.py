"""Unit tests for ExecutionManager.

Tests cover:
  - Config loading (happy path and error cases).
  - High-risk operation classification.
  - Confirmation workflow (confirmed / rejected / timeout).
  - Task termination (pre-action and mid-retry).
  - Retry logic for transient failures.
"""

from __future__ import annotations

import json
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cloud_ops_ai_agent.exceptions import (
    ConfigLoadError,
    ConfirmationRejectedError,
    ConfirmationTimeoutError,
    TaskTerminatedError,
    TaskTimeoutError,
)
from cloud_ops_ai_agent.execution_manager import ExecutionManager


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def _write_config(tmp_path: Path, overrides: dict | None = None) -> Path:
    """Write a config.json to *tmp_path*, applying *overrides* to defaults."""
    cfg: dict = {
        "high_risk_operations": ["delete_instance", "wipe_storage_bucket"],
        "confirmation": {
            "timeout_seconds": 5,
            "prompt_template": "Confirm '{operation}' on '{resource}' [{confirm_token}]: ",
            "confirm_token": "CONFIRM",
        },
        "execution": {
            "max_retries": 2,
            "retry_delay_seconds": 0.01,
            "task_timeout_seconds": 30,
        },
        "logging": {"level": "DEBUG", "audit_log_path": "logs/audit.log"},
    }
    if overrides:
        for key, value in overrides.items():
            if isinstance(value, dict) and isinstance(cfg.get(key), dict):
                cfg[key].update(value)
            else:
                cfg[key] = value
    p = tmp_path / "config.json"
    p.write_text(json.dumps(cfg), encoding="utf-8")
    return p


@pytest.fixture()
def config_file(tmp_path: Path) -> Path:
    """Write a minimal valid config.json to a temp directory."""
    return _write_config(tmp_path)


@pytest.fixture()
def manager(config_file: Path) -> ExecutionManager:
    return ExecutionManager(config_path=str(config_file))


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------


class TestConfigLoading:
    def test_loads_valid_config(self, manager: ExecutionManager) -> None:
        assert "delete_instance" in manager.config.high_risk_operations
        assert manager.config.max_retries == 2

    def test_raises_on_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigLoadError, match="File not found"):
            ExecutionManager(config_path=str(tmp_path / "nonexistent.json"))

    def test_raises_on_invalid_json(self, tmp_path: Path) -> None:
        bad = tmp_path / "bad.json"
        bad.write_text("{not valid json", encoding="utf-8")
        with pytest.raises(ConfigLoadError, match="Invalid JSON"):
            ExecutionManager(config_path=str(bad))

    def test_raises_on_missing_key(self, tmp_path: Path) -> None:
        incomplete = tmp_path / "incomplete.json"
        incomplete.write_text(json.dumps({"high_risk_operations": []}), encoding="utf-8")
        with pytest.raises(ConfigLoadError, match="Missing required key"):
            ExecutionManager(config_path=str(incomplete))


# ---------------------------------------------------------------------------
# High-risk classification
# ---------------------------------------------------------------------------


class TestHighRiskClassification:
    def test_known_high_risk_operation(self, manager: ExecutionManager) -> None:
        assert manager.is_high_risk_operation("delete_instance") is True

    def test_safe_operation(self, manager: ExecutionManager) -> None:
        assert manager.is_high_risk_operation("list_instances") is False

    def test_case_sensitive(self, manager: ExecutionManager) -> None:
        assert manager.is_high_risk_operation("Delete_Instance") is False


# ---------------------------------------------------------------------------
# Confirmation workflow
# ---------------------------------------------------------------------------


class TestConfirmationWorkflow:
    def test_confirmed_operation_proceeds(self, manager: ExecutionManager) -> None:
        action = MagicMock(return_value="ok")
        with patch("builtins.input", return_value="CONFIRM"):
            result = manager.execute_task(
                operation="delete_instance",
                resource="vm-1",
                action=action,
            )
        assert result == "ok"
        action.assert_called_once()

    def test_rejected_confirmation_raises(self, manager: ExecutionManager) -> None:
        action = MagicMock()
        with patch("builtins.input", return_value="no"):
            with pytest.raises(ConfirmationRejectedError):
                manager.execute_task(
                    operation="delete_instance",
                    resource="vm-1",
                    action=action,
                )
        action.assert_not_called()

    def test_confirmation_timeout_raises(self, tmp_path: Path) -> None:
        short_timeout_config = _write_config(
            tmp_path, overrides={"confirmation": {"timeout_seconds": 1}}
        )
        mgr = ExecutionManager(config_path=str(short_timeout_config))

        def _slow_input(_prompt: str) -> str:
            time.sleep(5)
            return "CONFIRM"

        action = MagicMock()
        with patch("builtins.input", side_effect=_slow_input):
            with pytest.raises(ConfirmationTimeoutError):
                mgr.execute_task(
                    operation="delete_instance",
                    resource="vm-2",
                    action=action,
                )
        action.assert_not_called()


# ---------------------------------------------------------------------------
# Task termination
# ---------------------------------------------------------------------------


class TestTaskTermination:
    def test_terminate_between_retries(self, manager: ExecutionManager) -> None:
        """Cancel a task between retry attempts; TaskTerminatedError must propagate."""
        # The action raises a transient error on the first attempt, then blocks.
        # Once the first attempt finishes, the main thread cancels the task so
        # the cancellation check at the top of the next retry fires.
        barrier = threading.Barrier(2)
        first_call = True

        def _flaky_action():
            nonlocal first_call
            if first_call:
                first_call = False
                barrier.wait()       # signal: first attempt done
                raise ConnectionError("transient")
            return "ok"

        task_id = "term-test-001"
        exc_holder: list[Exception] = []

        def _run() -> None:
            try:
                manager.execute_task(
                    operation="list_instances",
                    resource="all",
                    action=_flaky_action,
                    task_id=task_id,
                )
            except Exception as exc:  # pylint: disable=broad-except
                exc_holder.append(exc)

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        barrier.wait()               # wait until first attempt raised
        manager.terminate_task(task_id)
        t.join(timeout=3)

        assert len(exc_holder) == 1
        assert isinstance(exc_holder[0], TaskTerminatedError)

    def test_terminate_unknown_task_returns_false(
        self, manager: ExecutionManager
    ) -> None:
        assert manager.terminate_task("ghost-task") is False

    def test_terminate_completed_task_returns_false(
        self, manager: ExecutionManager
    ) -> None:
        task_id = "done-task"
        with patch("builtins.input", return_value="CONFIRM"):
            manager.execute_task(
                operation="list_instances",
                resource="r",
                action=lambda: None,
                task_id=task_id,
            )
        assert manager.terminate_task(task_id) is False


# ---------------------------------------------------------------------------
# Retry logic
# ---------------------------------------------------------------------------


class TestRetryLogic:
    def test_retries_on_transient_failure(self, manager: ExecutionManager) -> None:
        call_count = 0

        def _flaky_action():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("transient")
            return "recovered"

        result = manager.execute_task(
            operation="list_instances",
            resource="r",
            action=_flaky_action,
        )
        assert result == "recovered"
        assert call_count == 3

    def test_exhausted_retries_propagate_last_error(
        self, manager: ExecutionManager
    ) -> None:
        def _always_fails():
            raise ValueError("permanent failure")

        with pytest.raises(ValueError, match="permanent failure"):
            manager.execute_task(
                operation="list_instances",
                resource="r",
                action=_always_fails,
            )

    def test_timeout_raises_task_timeout_error(self, tmp_path: Path) -> None:
        # Each action attempt sleeps 30 ms.  With a 60 ms total budget and
        # many max_retries, the deadline fires before retries are exhausted.
        tight_config = _write_config(
            tmp_path,
            overrides={"execution": {
                "task_timeout_seconds": 0.06,
                "max_retries": 10,
                "retry_delay_seconds": 0.0,
            }},
        )
        mgr = ExecutionManager(config_path=str(tight_config))

        def _slow_failing():
            time.sleep(0.03)          # 30 ms each attempt → 2 attempts ≈ 60 ms
            raise ConnectionError("transient")

        with pytest.raises(TaskTimeoutError):
            mgr.execute_task(
                operation="list_instances",
                resource="r",
                action=_slow_failing,
            )


# ---------------------------------------------------------------------------
# Task registry
# ---------------------------------------------------------------------------


class TestTaskRegistry:
    def test_list_task_records_returns_submitted_tasks(
        self, manager: ExecutionManager
    ) -> None:
        manager.execute_task(
            operation="list_instances",
            resource="r",
            action=lambda: "ok",
        )
        records = manager.list_task_records()
        assert len(records) == 1
        assert records[0].operation == "list_instances"

    def test_cancel_flags_cleaned_up_after_completion(
        self, manager: ExecutionManager
    ) -> None:
        task_id = "cleanup-test"
        manager.execute_task(
            operation="list_instances",
            resource="r",
            action=lambda: None,
            task_id=task_id,
        )
        # _cancel_flags must be empty once the task reaches a terminal state.
        assert task_id not in manager._cancel_flags  # noqa: SLF001

    def test_config_is_immutable(self, manager: ExecutionManager) -> None:
        from dataclasses import FrozenInstanceError
        with pytest.raises(FrozenInstanceError):
            manager.config.max_retries = 99  # type: ignore[misc]
