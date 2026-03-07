"""
Unit tests for execution_manager — Async Industrial Execution Engine.

Covers: AppConfig, SafetyGateway (3-phase), CancellationToken,
@handle_cloud_exceptions, ExecutionManager single/batch dispatch,
and the typed exception hierarchy.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any, Dict

import pytest

from execution_manager import (
    AppConfig,
    CancellationToken,
    ExecutionManager,
    IdentityVerificationError,
    PermanentCloudError,
    S3LogUploader,
    SafetyGateway,
    SecurityViolationError,
    TaskCancelledError,
    TaskContext,
    TransientCloudError,
    create_engine,
    handle_cloud_exceptions,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CONFIG_PATH = Path(__file__).resolve().parent.parent / "config.json"


@pytest.fixture
def config() -> AppConfig:
    return AppConfig(str(CONFIG_PATH))


@pytest.fixture
def gateway(config: AppConfig) -> SafetyGateway:
    return SafetyGateway(config)


@pytest.fixture
def noop_s3() -> S3LogUploader:
    """S3 uploader with no bucket configured — upload_logs_to_s3 becomes a no-op."""
    return S3LogUploader(bucket="")


@pytest.fixture
def engine(config: AppConfig, noop_s3: S3LogUploader) -> ExecutionManager:
    return ExecutionManager(config, s3_uploader=noop_s3)


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
# AppConfig
# ---------------------------------------------------------------------------


class TestAppConfig:
    def test_loads_from_default_path(self, config: AppConfig) -> None:
        assert config.app_name == "cloud-ops-ai-agent"
        assert config.retry.max_attempts >= 1
        assert config.execution.batch_concurrency_limit > 0

    def test_risk_level_lookup(self, config: AppConfig) -> None:
        assert config.get_risk_level("delete_node") == "critical"
        assert config.get_risk_level("health_check") == "low"
        assert config.get_risk_level("restart_node") == "high"
        assert config.get_risk_level("nonexistent_op") is None

    def test_risk_profile_lookup(self, config: AppConfig) -> None:
        profile = config.get_risk_profile("delete_node")
        assert profile is not None
        assert profile.requires_identity_verification is True
        assert profile.requires_mfa is True

    def test_frozen_dataclasses(self, config: AppConfig) -> None:
        with pytest.raises(AttributeError):
            config.retry.max_attempts = 999  # type: ignore[misc]

    def test_custom_config_path(self, tmp_path: Path) -> None:
        custom = tmp_path / "custom.json"
        raw = json.loads(CONFIG_PATH.read_text())
        raw["app"]["name"] = "custom-app"
        custom.write_text(json.dumps(raw))

        cfg = AppConfig(str(custom))
        assert cfg.app_name == "custom-app"


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class TestExceptions:
    def test_transient_is_retryable(self) -> None:
        exc = TransientCloudError("network blip", trace_id="t1")
        assert exc.retryable is True

    def test_permanent_not_retryable(self) -> None:
        exc = PermanentCloudError("bad request", trace_id="t2")
        assert exc.retryable is False

    def test_security_inherits_permanent(self) -> None:
        exc = SecurityViolationError("blocked")
        assert isinstance(exc, PermanentCloudError)

    def test_identity_inherits_security(self) -> None:
        exc = IdentityVerificationError("mfa fail")
        assert isinstance(exc, SecurityViolationError)

    def test_cancelled_not_retryable(self) -> None:
        exc = TaskCancelledError(trace_id="t3")
        assert exc.retryable is False
        assert "cancelled" in str(exc).lower()


# ---------------------------------------------------------------------------
# CancellationToken
# ---------------------------------------------------------------------------


class TestCancellationToken:
    def test_initial_state(self) -> None:
        token = CancellationToken("trace-1")
        assert token.is_cancelled is False
        assert token.reason is None

    def test_cancel_sets_flag(self) -> None:
        token = CancellationToken("trace-2")
        token.cancel(reason="operator abort")
        assert token.is_cancelled is True
        assert token.reason == "operator abort"

    def test_raise_if_cancelled(self) -> None:
        token = CancellationToken("trace-3")
        token.raise_if_cancelled()  # should not raise

        token.cancel()
        with pytest.raises(TaskCancelledError):
            token.raise_if_cancelled()

    def test_cancel_is_idempotent(self) -> None:
        token = CancellationToken("trace-4")
        token.cancel(reason="first")
        token.cancel(reason="second")
        assert token.reason == "first"  # first reason preserved


# ---------------------------------------------------------------------------
# SafetyGateway
# ---------------------------------------------------------------------------


class TestSafetyGateway:
    @pytest.mark.asyncio
    async def test_low_risk_no_verification(self, gateway: SafetyGateway) -> None:
        ctx = _ctx("health_check")
        await gateway.check_operation("health_check", ctx)

    @pytest.mark.asyncio
    async def test_high_risk_requires_identity(self, gateway: SafetyGateway) -> None:
        ctx = _ctx("restart_node", session_token="valid")
        await gateway.check_operation("restart_node", ctx)

    @pytest.mark.asyncio
    async def test_high_risk_fails_without_token(
        self, gateway: SafetyGateway
    ) -> None:
        ctx = _ctx("restart_node", session_token="")
        with pytest.raises(IdentityVerificationError):
            await gateway.check_operation("restart_node", ctx)

    @pytest.mark.asyncio
    async def test_critical_requires_mfa(self, gateway: SafetyGateway) -> None:
        ctx = _ctx("delete_node", session_token="valid", mfa_token="123456")
        await gateway.check_operation("delete_node", ctx)

    @pytest.mark.asyncio
    async def test_critical_fails_without_mfa(
        self, gateway: SafetyGateway
    ) -> None:
        ctx = _ctx("delete_node", session_token="valid", mfa_token=None)
        with pytest.raises(IdentityVerificationError, match="MFA"):
            await gateway.check_operation("delete_node", ctx)

    @pytest.mark.asyncio
    async def test_unknown_op_defaults_high(
        self, gateway: SafetyGateway
    ) -> None:
        ctx = _ctx("totally_unknown_op", session_token="valid")
        await gateway.check_operation("totally_unknown_op", ctx)


# ---------------------------------------------------------------------------
# @handle_cloud_exceptions decorator
# ---------------------------------------------------------------------------


class TestHandleCloudExceptions:
    @pytest.mark.asyncio
    async def test_success_passes_through(self, config: AppConfig) -> None:
        class Fake:
            _config = config

            @handle_cloud_exceptions
            async def do_work(self, ctx: TaskContext) -> str:
                return "ok"

        result = await Fake().do_work(_ctx())
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_transient_error_retried(self, config: AppConfig) -> None:
        call_count = 0

        class Fake:
            _config = config

            @handle_cloud_exceptions
            async def do_work(self, ctx: TaskContext) -> str:
                nonlocal call_count
                call_count += 1
                if call_count < 3:
                    raise TransientCloudError("blip", trace_id=ctx.trace_id)
                return "recovered"

        result = await Fake().do_work(_ctx())
        assert result == "recovered"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_permanent_error_not_retried(self, config: AppConfig) -> None:
        call_count = 0

        class Fake:
            _config = config

            @handle_cloud_exceptions
            async def do_work(self, ctx: TaskContext) -> str:
                nonlocal call_count
                call_count += 1
                raise PermanentCloudError("fatal", trace_id=ctx.trace_id)

        with pytest.raises(PermanentCloudError):
            await Fake().do_work(_ctx())
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_cancelled_propagates_immediately(
        self, config: AppConfig
    ) -> None:
        class Fake:
            _config = config

            @handle_cloud_exceptions
            async def do_work(self, ctx: TaskContext) -> str:
                raise TaskCancelledError(trace_id=ctx.trace_id)

        with pytest.raises(TaskCancelledError):
            await Fake().do_work(_ctx())

    @pytest.mark.asyncio
    async def test_unexpected_error_wrapped(self, config: AppConfig) -> None:
        class Fake:
            _config = config

            @handle_cloud_exceptions
            async def do_work(self, ctx: TaskContext) -> str:
                raise RuntimeError("something weird")

        with pytest.raises(PermanentCloudError, match="something weird"):
            await Fake().do_work(_ctx())


# ---------------------------------------------------------------------------
# ExecutionManager — single task
# ---------------------------------------------------------------------------


class TestExecutionManager:
    @pytest.mark.asyncio
    async def test_health_check(self, engine: ExecutionManager) -> None:
        result = await engine.execute(_ctx("health_check"))
        assert result["healthy"] is True
        assert "latency_ms" in result

    @pytest.mark.asyncio
    async def test_restart_node(self, engine: ExecutionManager) -> None:
        ctx = _ctx(
            "restart_node",
            session_token="valid",
            payload={"node_id": "n1"},
        )
        result = await engine.execute(ctx)
        assert result["restarted"] == "n1"

    @pytest.mark.asyncio
    async def test_delete_node_critical(self, engine: ExecutionManager) -> None:
        ctx = _ctx(
            "delete_node",
            session_token="valid",
            mfa_token="code",
            payload={"node_id": "n-doom"},
        )
        result = await engine.execute(ctx)
        assert result["deleted"] == "n-doom"

    @pytest.mark.asyncio
    async def test_delete_node_rejected_without_mfa(
        self, engine: ExecutionManager
    ) -> None:
        ctx = _ctx(
            "delete_node",
            session_token="valid",
            mfa_token=None,
            payload={"node_id": "n-safe"},
        )
        with pytest.raises(IdentityVerificationError):
            await engine.execute(ctx)

    @pytest.mark.asyncio
    async def test_collect_metrics(self, engine: ExecutionManager) -> None:
        result = await engine.execute(_ctx("collect_metrics"))
        assert "cpu_avg_pct" in result

    @pytest.mark.asyncio
    async def test_generic_fallback(self, engine: ExecutionManager) -> None:
        ctx = _ctx("some_new_operation", session_token="valid")
        result = await engine.execute(ctx)
        assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# ExecutionManager — cancellation
# ---------------------------------------------------------------------------


class TestCancellation:
    @pytest.mark.asyncio
    async def test_cancel_mid_batch_update(
        self, engine: ExecutionManager
    ) -> None:
        ctx = _ctx(
            "batch_update_devices",
            payload={"device_ids": [f"d-{i}" for i in range(20)]},
        )

        async def _cancel_soon() -> None:
            await asyncio.sleep(0.3)
            engine.cancel_task(ctx.trace_id, reason="test abort")

        asyncio.create_task(_cancel_soon())
        with pytest.raises(TaskCancelledError):
            await engine.execute(ctx)

    def test_cancel_nonexistent_trace(self, engine: ExecutionManager) -> None:
        assert engine.cancel_task("nonexistent-trace") is False


# ---------------------------------------------------------------------------
# ExecutionManager — batch
# ---------------------------------------------------------------------------


class TestBatchExecution:
    @pytest.mark.asyncio
    async def test_batch_all_succeed(self, engine: ExecutionManager) -> None:
        contexts = [
            _ctx("health_check"),
            _ctx("collect_metrics"),
        ]
        results = await engine.execute_batch(contexts)
        assert len(results) == 2
        assert all(r["status"] == "success" for r in results)

    @pytest.mark.asyncio
    async def test_batch_partial_failure(
        self, engine: ExecutionManager
    ) -> None:
        contexts = [
            _ctx("health_check"),
            _ctx("delete_node", session_token="valid", mfa_token=None),
        ]
        results = await engine.execute_batch(contexts)
        assert results[0]["status"] == "success"
        assert results[1]["status"] == "error"

    @pytest.mark.asyncio
    async def test_batch_fail_fast(self, engine: ExecutionManager) -> None:
        contexts = [
            _ctx("delete_node", session_token="", mfa_token=None),
            _ctx("health_check"),
            _ctx("health_check"),
        ]
        results = await engine.execute_batch(contexts, fail_fast=True)
        statuses = [r["status"] for r in results]
        assert "error" in statuses


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


class TestFactory:
    def test_create_engine(self) -> None:
        eng = create_engine(str(CONFIG_PATH))
        assert isinstance(eng, ExecutionManager)
