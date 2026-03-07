"""
Unit tests for metrics_collector — MetricsRegistry async singleton.

Covers: singleton lifecycle, counter recording, histogram statistics,
Prometheus text exposition, and end-to-end integration with
ExecutionManager.
"""

from __future__ import annotations

import asyncio
from typing import Any, Dict

import pytest

from execution_manager import (
    AppConfig,
    ExecutionManager,
    IdentityVerificationError,
    S3LogUploader,
    TaskCancelledError,
    TaskContext,
)
from metrics_collector import MetricsRegistry

CONFIG_PATH = "config.json"


@pytest.fixture(autouse=True)
def _fresh_registry():
    """Ensure every test gets a clean MetricsRegistry."""
    MetricsRegistry.reset()
    yield
    MetricsRegistry.reset()


def _ctx(
    operation: str = "health_check",
    *,
    session_token: str = "valid-token",
    mfa_token: str | None = None,
    payload: Dict[str, Any] | None = None,
) -> TaskContext:
    return TaskContext(
        operation=operation,
        payload=payload or {},
        operator_id="test-user",
        session_token=session_token,
        mfa_token=mfa_token,
    )


# ---------------------------------------------------------------------------
# Singleton lifecycle
# ---------------------------------------------------------------------------


class TestSingleton:
    def test_get_returns_same_instance(self) -> None:
        a = MetricsRegistry.get()
        b = MetricsRegistry.get()
        assert a is b

    def test_create_resets_instance(self) -> None:
        old = MetricsRegistry.get()
        new = MetricsRegistry.create()
        assert new is not old
        assert MetricsRegistry.get() is new

    def test_reset_clears_instance(self) -> None:
        MetricsRegistry.get()
        MetricsRegistry.reset()
        assert MetricsRegistry._instance is None


# ---------------------------------------------------------------------------
# Counter recording
# ---------------------------------------------------------------------------


class TestCounters:
    @pytest.mark.asyncio
    async def test_record_success(self) -> None:
        reg = MetricsRegistry.get()
        await reg.record_task_success("health_check", 42.0)

        snap = await reg.snapshot()
        assert snap["task_counters"]["health_check|success"] == 1

    @pytest.mark.asyncio
    async def test_record_failure(self) -> None:
        reg = MetricsRegistry.get()
        await reg.record_task_failure("delete_node", 100.0)

        snap = await reg.snapshot()
        assert snap["task_counters"]["delete_node|failed"] == 1

    @pytest.mark.asyncio
    async def test_record_cancelled(self) -> None:
        reg = MetricsRegistry.get()
        await reg.record_task_cancelled("batch_update_devices", 500.0)

        snap = await reg.snapshot()
        assert snap["task_counters"]["batch_update_devices|cancelled"] == 1

    @pytest.mark.asyncio
    async def test_record_safety_interception(self) -> None:
        reg = MetricsRegistry.get()
        await reg.record_safety_interception("delete_node", "critical")

        snap = await reg.snapshot()
        assert snap["safety_interceptions"]["delete_node|critical"] == 1

    @pytest.mark.asyncio
    async def test_multiple_increments(self) -> None:
        reg = MetricsRegistry.get()
        for _ in range(5):
            await reg.record_task_success("health_check", 10.0)

        snap = await reg.snapshot()
        assert snap["task_counters"]["health_check|success"] == 5


# ---------------------------------------------------------------------------
# Active tasks gauge
# ---------------------------------------------------------------------------


class TestActiveTasksGauge:
    @pytest.mark.asyncio
    async def test_start_increments(self) -> None:
        reg = MetricsRegistry.get()
        await reg.record_task_start()
        await reg.record_task_start()
        snap = await reg.snapshot()
        assert snap["active_tasks"] == 2

    @pytest.mark.asyncio
    async def test_completion_decrements(self) -> None:
        reg = MetricsRegistry.get()
        await reg.record_task_start()
        await reg.record_task_start()
        await reg.record_task_success("op", 10.0)
        snap = await reg.snapshot()
        assert snap["active_tasks"] == 1

    @pytest.mark.asyncio
    async def test_never_goes_negative(self) -> None:
        reg = MetricsRegistry.get()
        await reg.record_task_success("op", 10.0)
        snap = await reg.snapshot()
        assert snap["active_tasks"] == 0


# ---------------------------------------------------------------------------
# Histogram / duration tracking
# ---------------------------------------------------------------------------


class TestHistogram:
    @pytest.mark.asyncio
    async def test_avg_calculation(self) -> None:
        reg = MetricsRegistry.get()
        await reg.record_task_success("op", 100.0)  # 0.1s
        await reg.record_task_success("op", 300.0)  # 0.3s

        snap = await reg.snapshot()
        hist = snap["duration_histograms"]["op"]
        assert hist["count"] == 2
        assert abs(hist["avg_seconds"] - 0.2) < 0.001

    @pytest.mark.asyncio
    async def test_min_max(self) -> None:
        reg = MetricsRegistry.get()
        await reg.record_task_success("op", 50.0)
        await reg.record_task_success("op", 200.0)
        await reg.record_task_success("op", 100.0)

        snap = await reg.snapshot()
        hist = snap["duration_histograms"]["op"]
        assert abs(hist["min_seconds"] - 0.05) < 0.001
        assert abs(hist["max_seconds"] - 0.2) < 0.001


# ---------------------------------------------------------------------------
# Prometheus text exposition
# ---------------------------------------------------------------------------


class TestPrometheusFormat:
    @pytest.mark.asyncio
    async def test_contains_required_sections(self) -> None:
        reg = MetricsRegistry.get()
        await reg.record_task_success("health_check", 42.0)
        await reg.record_safety_interception("delete_node", "critical")

        output = reg.to_prometheus_format()

        assert "# HELP cloudops_tasks_total" in output
        assert "# TYPE cloudops_tasks_total counter" in output
        assert 'cloudops_tasks_total{operation="health_check"' in output

        assert "# HELP cloudops_safety_interceptions_total" in output
        assert 'cloudops_safety_interceptions_total{operation="delete_node"' in output

        assert "# HELP cloudops_task_duration_seconds" in output
        assert "cloudops_task_duration_seconds_sum" in output
        assert "cloudops_task_duration_seconds_count" in output
        assert "cloudops_task_duration_seconds_avg" in output

        assert "# HELP cloudops_active_tasks" in output
        assert "# TYPE cloudops_active_tasks gauge" in output

        assert "# HELP cloudops_uptime_seconds" in output

    @pytest.mark.asyncio
    async def test_empty_registry_valid_format(self) -> None:
        reg = MetricsRegistry.get()
        output = reg.to_prometheus_format()
        assert "# HELP cloudops_tasks_total" in output
        assert "cloudops_active_tasks 0" in output


# ---------------------------------------------------------------------------
# End-to-end integration with ExecutionManager
# ---------------------------------------------------------------------------


class TestIntegrationWithEngine:
    @pytest.fixture
    def engine(self) -> ExecutionManager:
        cfg = AppConfig(CONFIG_PATH)
        registry = MetricsRegistry.get()
        noop_s3 = S3LogUploader(bucket="")
        return ExecutionManager(cfg, metrics=registry, s3_uploader=noop_s3)

    @pytest.mark.asyncio
    async def test_success_updates_metrics(
        self, engine: ExecutionManager
    ) -> None:
        await engine.execute(_ctx("health_check"))

        snap = await MetricsRegistry.get().snapshot()
        assert snap["task_counters"]["health_check|success"] == 1
        assert "health_check" in snap["duration_histograms"]
        assert snap["active_tasks"] == 0

    @pytest.mark.asyncio
    async def test_safety_interception_updates_metrics(
        self, engine: ExecutionManager
    ) -> None:
        ctx = _ctx("delete_node", session_token="valid", mfa_token=None)
        with pytest.raises(IdentityVerificationError):
            await engine.execute(ctx)

        snap = await MetricsRegistry.get().snapshot()
        assert snap["safety_interceptions"]["delete_node|critical"] == 1
        assert snap["task_counters"]["delete_node|failed"] == 1

    @pytest.mark.asyncio
    async def test_cancel_updates_metrics(
        self, engine: ExecutionManager
    ) -> None:
        ctx = _ctx(
            "batch_update_devices",
            payload={"device_ids": [f"d-{i}" for i in range(20)]},
        )

        async def _cancel_soon() -> None:
            await asyncio.sleep(0.3)
            engine.cancel_task(ctx.trace_id, reason="test")

        asyncio.create_task(_cancel_soon())
        with pytest.raises(TaskCancelledError):
            await engine.execute(ctx)

        snap = await MetricsRegistry.get().snapshot()
        assert snap["task_counters"]["batch_update_devices|cancelled"] == 1

    @pytest.mark.asyncio
    async def test_prometheus_output_after_mixed_ops(
        self, engine: ExecutionManager
    ) -> None:
        await engine.execute(_ctx("health_check"))
        await engine.execute(_ctx("collect_metrics"))

        ctx_fail = _ctx("delete_node", session_token="valid", mfa_token=None)
        with pytest.raises(IdentityVerificationError):
            await engine.execute(ctx_fail)

        output = MetricsRegistry.get().to_prometheus_format()

        assert 'status="success"' in output
        assert 'status="failed"' in output
        assert 'risk_level="critical"' in output
        assert "cloudops_active_tasks 0" in output
