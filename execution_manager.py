"""
execution_manager.py — Async Industrial Execution Engine for cloud-ops-ai-agent.

Refactored based on Baidu Cloud-Phone scalable architecture patterns.

This module implements the core async execution engine, inspired by the
multi-device batch management requirements of Baidu's Cloud-Phone (红手指) platform.
It handles parallel device orchestration, safety-gated operations, graceful
cancellation, and config-driven retry/timeout policies — all built for
high-concurrency, large-scale cloud operations.

Architecture layers
-------------------
AppConfig              → Single source of truth for all tunable parameters.
CloudOpsError          → Typed exception hierarchy for structured error handling.
@handle_cloud_exceptions → Method decorator: exponential-backoff retry, no hard-codes.
SafetyGateway          → Three-phase identity & MFA verification for risky ops.
CancellationToken      → asyncio.Event cooperative task termination.
TaskContext            → Per-task envelope carrying TraceID, metadata, timestamps.
ExecutionManager       → Orchestrator: dispatches, monitors, and audits all ops.

RAG integration note
---------------------
Each public method is documented with its intent and data contracts so that
future RAG retrieval can surface exact procedures from the knowledge base.
The ``rag_hint`` field on TaskContext is reserved for injected RAG context.
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from pathlib import Path
from typing import (
    Any, Awaitable, Callable, Dict, List, Optional, Sequence, TypeVar,
)

import boto3
from botocore.exceptions import BotoCoreError, ClientError

from metrics_collector import MetricsRegistry

# ---------------------------------------------------------------------------
# Logging bootstrap — structured single-line format for log-aggregation pipelines
# ---------------------------------------------------------------------------

_LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | trace=%(trace_id)s | %(name)s | %(message)s"
)


class _TraceFilter(logging.Filter):
    """Inject a default trace_id into every LogRecord that lacks one."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        if not hasattr(record, "trace_id"):
            record.trace_id = "N/A"
        return True


def _build_logger(name: str, level: str = "INFO") -> logging.Logger:
    log = logging.getLogger(name)
    if not log.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(_LOG_FORMAT))
        handler.addFilter(_TraceFilter())
        log.addHandler(handler)
    log.setLevel(getattr(logging, level.upper(), logging.INFO))
    return log


logger = _build_logger(__name__)


def _trace_logger(trace_id: str) -> logging.LoggerAdapter:
    """Return a LoggerAdapter that automatically stamps every message with *trace_id*."""
    return logging.LoggerAdapter(logger, {"trace_id": trace_id})


# ---------------------------------------------------------------------------
# AppConfig — config-driven design, zero hard-coded magic numbers
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RetryConfig:
    """Retry policy sourced exclusively from config.json."""

    max_attempts: int
    base_backoff_seconds: float
    max_backoff_seconds: float
    backoff_multiplier: float
    jitter_factor: float


@dataclass(frozen=True)
class ExecutionConfig:
    """Execution scheduling parameters."""

    default_timeout_seconds: float
    long_task_timeout_seconds: float
    batch_concurrency_limit: int
    cancellation_check_interval_seconds: float


@dataclass(frozen=True)
class SafetyConfig:
    """Identity-verification parameters for SafetyGateway."""

    identity_verification_timeout_seconds: float
    session_token_ttl_seconds: int
    mfa_required_for_critical: bool


@dataclass(frozen=True)
class RiskProfile:
    """Per-operation-class risk constraints loaded from config.json."""

    operations: List[str]
    requires_identity_verification: bool
    requires_mfa: bool
    audit_log: bool
    cooldown_seconds: float


@dataclass(frozen=True)
class RagConfig:
    """RAG knowledge-base integration parameters (reserved for future use)."""

    enabled: bool
    knowledge_base_url: str
    embedding_model: str
    top_k_results: int
    similarity_threshold: float


class AppConfig:
    """
    Central configuration loader.

    Reads config.json once at construction time and exposes typed,
    immutable sub-configs.  All engine components receive this object
    rather than accessing the file directly — the canonical backend-configurable
    pattern from Baidu's Cloud-Phone architecture.

    Parameters
    ----------
    config_path : str | None
        Path to config.json.  Defaults to the file co-located with this module.
    """

    def __init__(self, config_path: Optional[str] = None) -> None:
        resolved = Path(config_path or Path(__file__).parent / "config.json")
        with resolved.open("r", encoding="utf-8") as fh:
            raw: Dict[str, Any] = json.load(fh)

        self.app_name: str = raw["app"]["name"]
        self.log_level: str = raw["app"]["log_level"]

        r = raw["retry"]
        self.retry = RetryConfig(
            max_attempts=r["max_attempts"],
            base_backoff_seconds=r["base_backoff_seconds"],
            max_backoff_seconds=r["max_backoff_seconds"],
            backoff_multiplier=r["backoff_multiplier"],
            jitter_factor=r["jitter_factor"],
        )

        e = raw["execution"]
        self.execution = ExecutionConfig(
            default_timeout_seconds=e["default_timeout_seconds"],
            long_task_timeout_seconds=e["long_task_timeout_seconds"],
            batch_concurrency_limit=e["batch_concurrency_limit"],
            cancellation_check_interval_seconds=e["cancellation_check_interval_seconds"],
        )

        sg = raw["safety_gateway"]
        self.safety = SafetyConfig(
            identity_verification_timeout_seconds=sg["identity_verification_timeout_seconds"],
            session_token_ttl_seconds=sg["session_token_ttl_seconds"],
            mfa_required_for_critical=sg["mfa_required_for_critical"],
        )

        self.risk_profiles: Dict[str, RiskProfile] = {
            level: RiskProfile(
                operations=data["operations"],
                requires_identity_verification=data["requires_identity_verification"],
                requires_mfa=data["requires_mfa"],
                audit_log=data["audit_log"],
                cooldown_seconds=data["cooldown_seconds"],
            )
            for level, data in raw["risk_levels"].items()
        }

        rag = raw["rag_integration"]
        self.rag = RagConfig(
            enabled=rag["enabled"],
            knowledge_base_url=rag["knowledge_base_url"],
            embedding_model=rag["embedding_model"],
            top_k_results=rag["top_k_results"],
            similarity_threshold=rag["similarity_threshold"],
        )

    def get_risk_level(self, operation: str) -> Optional[str]:
        """Return the risk-level string for *operation*, or None if uncategorised."""
        for level, profile in self.risk_profiles.items():
            if operation in profile.operations:
                return level
        return None

    def get_risk_profile(self, operation: str) -> Optional[RiskProfile]:
        """Return the full RiskProfile for *operation*."""
        level = self.get_risk_level(operation)
        return self.risk_profiles.get(level) if level else None


# ---------------------------------------------------------------------------
# Exception hierarchy — typed errors for structured upstream handling
# ---------------------------------------------------------------------------


class CloudOpsError(Exception):
    """Base class for all cloud-ops-ai-agent exceptions."""

    def __init__(
        self, message: str, trace_id: str = "N/A", retryable: bool = False
    ) -> None:
        super().__init__(message)
        self.trace_id = trace_id
        self.retryable = retryable


class TransientCloudError(CloudOpsError):
    """Network blips, temporary unavailability — safe to retry."""

    def __init__(self, message: str, trace_id: str = "N/A") -> None:
        super().__init__(message, trace_id=trace_id, retryable=True)


class PermanentCloudError(CloudOpsError):
    """Unrecoverable failures — must not be retried."""

    def __init__(self, message: str, trace_id: str = "N/A") -> None:
        super().__init__(message, trace_id=trace_id, retryable=False)


class SecurityViolationError(PermanentCloudError):
    """Raised when an operation is blocked by the SafetyGateway."""


class IdentityVerificationError(SecurityViolationError):
    """Raised when identity or MFA verification fails or times out."""


class TaskCancelledError(CloudOpsError):
    """Raised when a task is cooperatively cancelled via CancellationToken."""

    def __init__(self, trace_id: str = "N/A") -> None:
        super().__init__(
            "Task was gracefully cancelled.", trace_id=trace_id, retryable=False
        )


# ---------------------------------------------------------------------------
# @handle_cloud_exceptions — method decorator with exponential backoff retry
# ---------------------------------------------------------------------------

_AnyMethod = TypeVar(
    "_AnyMethod",
    bound=Callable[..., Awaitable[Any]],
)


def handle_cloud_exceptions(method: _AnyMethod) -> _AnyMethod:
    """
    Async method decorator that combines:

    * **Typed exception catching** — only ``TransientCloudError`` is retried;
      ``PermanentCloudError`` and ``TaskCancelledError`` propagate immediately.
    * **Exponential backoff with jitter**:
      ``delay = min(base * multiplier^attempt, max) * (1 + jitter * rand)``
    * **Config-driven parameters** — all values read from ``self._config.retry``
      at call time, never hard-coded.
    * **TraceID-stamped logging** on every retry and failure.

    Intended for use on ``ExecutionManager`` instance methods whose first
    positional argument (after ``self``) is a ``TaskContext``.

    Usage
    -----
    ::

        @handle_cloud_exceptions
        async def _op_restart_node(self, ctx: TaskContext, token: CancellationToken):
            ...
    """

    @functools.wraps(method)
    async def wrapper(self: Any, ctx: "TaskContext", *args: Any, **kwargs: Any) -> Any:
        rc: RetryConfig = self._config.retry
        tlog = _trace_logger(ctx.trace_id)

        for attempt in range(1, rc.max_attempts + 1):
            try:
                return await method(self, ctx, *args, **kwargs)

            except TaskCancelledError:
                raise  # cooperative cancellation must never be swallowed

            except TransientCloudError as exc:
                if attempt == rc.max_attempts:
                    tlog.error(
                        "Exhausted %d retries for '%s': %s",
                        rc.max_attempts,
                        method.__name__,
                        exc,
                    )
                    raise

                raw_delay = min(
                    rc.base_backoff_seconds * (rc.backoff_multiplier ** (attempt - 1)),
                    rc.max_backoff_seconds,
                )
                jitter = raw_delay * rc.jitter_factor * random.random()
                delay = raw_delay + jitter

                tlog.warning(
                    "Transient error on attempt %d/%d for '%s'. "
                    "Retrying in %.3fs — %s",
                    attempt,
                    rc.max_attempts,
                    method.__name__,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)

            except PermanentCloudError:
                raise  # never retry unrecoverable failures

            except asyncio.CancelledError:
                tlog.warning(
                    "asyncio.CancelledError propagated from '%s'.", method.__name__
                )
                raise

            except Exception as exc:
                tlog.exception("Unexpected error in '%s': %s", method.__name__, exc)
                raise PermanentCloudError(str(exc), trace_id=ctx.trace_id) from exc

        raise PermanentCloudError(
            f"All retries exhausted for {method.__name__}",
            trace_id=ctx.trace_id,
        )

    return wrapper  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# CancellationToken — cooperative task abort via asyncio.Event
# ---------------------------------------------------------------------------


class CancellationToken:
    """
    Cooperative cancellation signal backed by ``asyncio.Event``.

    Long-running tasks MUST call ``raise_if_cancelled()`` at every logical
    checkpoint — this mirrors the task-abort confirmation flow designed in
    Baidu's Cloud-Phone management console, where operators can stop any
    in-flight batch operation without forcing a hard kill.

    Attributes
    ----------
    trace_id : str
        Scopes the token to a single task for audit traceability.
    reason : str | None
        Human-readable reason set by the requester at cancellation time.
    """

    def __init__(self, trace_id: str) -> None:
        self.trace_id = trace_id
        self.reason: Optional[str] = None
        self._event: asyncio.Event = asyncio.Event()

    @property
    def is_cancelled(self) -> bool:
        return self._event.is_set()

    def cancel(self, reason: str = "operator requested") -> None:
        """Signal cancellation.  Idempotent — safe to call multiple times."""
        if not self._event.is_set():
            self.reason = reason
            self._event.set()
            _trace_logger(self.trace_id).warning(
                "CancellationToken activated — reason: '%s'", reason
            )

    def raise_if_cancelled(self) -> None:
        """
        Raise ``TaskCancelledError`` if cancellation was requested.

        Call this at every checkpoint inside long-running coroutines to
        ensure the abort is honoured without busy-waiting.
        """
        if self._event.is_set():
            raise TaskCancelledError(trace_id=self.trace_id)

    async def wait(self) -> None:
        """Block until the cancellation signal is set."""
        await self._event.wait()


# ---------------------------------------------------------------------------
# SafetyGateway — three-phase security verification
# ---------------------------------------------------------------------------


class VerificationPhase(Enum):
    """Phases of the three-stage confirmation flow."""

    RISK_CLASSIFICATION = auto()
    IDENTITY_VERIFICATION = auto()
    MFA_CONFIRMATION = auto()


class SafetyGateway:
    """
    Three-phase security gateway for high-risk cloud operations.

    Phase 1 — Risk classification
        Maps the operation name to a risk level defined in config.json.
        Unknown operations default to HIGH risk.

    Phase 2 — Identity verification
        Async token/session validation against the IdP.  A short-lived
        session cache avoids repeated round-trips within the same task.

    Phase 3 — MFA confirmation
        Time-based OTP or push-notification check for CRITICAL operations.
        Corresponds to the account-security and multi-factor verification
        mechanism built during Baidu Cloud-Phone internship.

    Parameters
    ----------
    config : AppConfig
        Runtime configuration — all timeouts are sourced from here.
    """

    def __init__(self, config: AppConfig) -> None:
        self._config = config
        self._verified_sessions: Dict[str, float] = {}  # trace_id → verified_at (monotonic)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def check_operation(self, operation: str, ctx: "TaskContext") -> None:
        """
        Run all applicable safety phases for *operation*.

        Raises
        ------
        SecurityViolationError
            If the operation is blocked at the policy level.
        IdentityVerificationError
            If identity or MFA verification fails.
        """
        tlog = _trace_logger(ctx.trace_id)
        profile = self._config.get_risk_profile(operation)

        if profile is None:
            tlog.warning(
                "SafetyGateway | Phase 1: op='%s' has no risk profile — "
                "defaulting to HIGH.",
                operation,
            )
            profile = self._config.risk_profiles["high"]
            risk_level = "high"
        else:
            risk_level = self._config.get_risk_level(operation) or "high"

        tlog.info(
            "SafetyGateway | Phase 1 complete | op='%s' risk='%s'",
            operation,
            risk_level,
        )

        if profile.requires_identity_verification:
            await self._verify_identity(ctx, risk_level)

        if profile.requires_mfa and self._config.safety.mfa_required_for_critical:
            await self._verify_mfa(ctx)

        if profile.cooldown_seconds > 0:
            tlog.info(
                "SafetyGateway | cooldown %.1fs for risk='%s'",
                profile.cooldown_seconds,
                risk_level,
            )
            await asyncio.sleep(profile.cooldown_seconds)

        tlog.info(
            "SafetyGateway | ALL phases passed | op='%s' cleared for execution.",
            operation,
        )

    # ------------------------------------------------------------------
    # Internal phase implementations
    # ------------------------------------------------------------------

    async def _verify_identity(self, ctx: "TaskContext", risk_level: str) -> None:
        """
        Phase 2 — async identity verification.

        In production, replace ``_idp_verify`` with a call to your IdP
        (e.g. Baidu SSO / internal OAuth2 introspection endpoint).
        Session results are cached for ``session_token_ttl_seconds`` to
        reduce IdP load under high-concurrency batch operations.
        """
        tlog = _trace_logger(ctx.trace_id)
        timeout = self._config.safety.identity_verification_timeout_seconds

        tlog.info(
            "SafetyGateway | Phase 2 — identity verify | risk='%s' timeout=%.1fs",
            risk_level,
            timeout,
        )

        cached_at = self._verified_sessions.get(ctx.trace_id)
        ttl = self._config.safety.session_token_ttl_seconds
        if cached_at and (time.monotonic() - cached_at) < ttl:
            tlog.info(
                "SafetyGateway | Phase 2 cache hit | trace=%s", ctx.trace_id
            )
            return

        try:
            verified = await asyncio.wait_for(
                self._idp_verify(ctx.operator_id, ctx.session_token),
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            raise IdentityVerificationError(
                f"Identity verification timed out after {timeout}s",
                trace_id=ctx.trace_id,
            ) from exc

        if not verified:
            raise IdentityVerificationError(
                f"Identity verification failed for operator='{ctx.operator_id}'",
                trace_id=ctx.trace_id,
            )

        self._verified_sessions[ctx.trace_id] = time.monotonic()
        tlog.info("SafetyGateway | Phase 2 passed — identity verified.")

    async def _verify_mfa(self, ctx: "TaskContext") -> None:
        """
        Phase 3 — MFA confirmation for CRITICAL operations.

        Stub: wire this to your TOTP or push-notification service in production.
        """
        tlog = _trace_logger(ctx.trace_id)
        tlog.info("SafetyGateway | Phase 3 — MFA confirmation for CRITICAL op.")

        if not ctx.mfa_token:
            raise IdentityVerificationError(
                "MFA token required for CRITICAL operation but was not provided.",
                trace_id=ctx.trace_id,
            )

        await asyncio.sleep(0.05)  # simulated TOTP/push verification round-trip
        tlog.info("SafetyGateway | Phase 3 passed — MFA confirmed.")

    @staticmethod
    async def _idp_verify(operator_id: str, session_token: Optional[str]) -> bool:
        """
        Mock IdP verification call.

        Replace with a real HTTP/gRPC call in production.
        Returns True when a non-empty session token is supplied.
        """
        await asyncio.sleep(0.05)  # simulated network I/O
        return bool(session_token)


# ---------------------------------------------------------------------------
# TaskContext — per-task envelope carrying TraceID and all metadata
# ---------------------------------------------------------------------------


@dataclass
class TaskContext:
    """
    Immutable execution envelope passed through every async call chain.

    Every field is intentionally public so that RAG post-processors can
    surface complete context when diagnosing failures or generating runbooks.

    Parameters
    ----------
    operation : str
        Logical operation name matching a key in config.json risk_levels.
    payload : dict
        Arbitrary, operation-specific parameters.
    operator_id : str
        ID of the human or service account initiating this task.
    session_token : str | None
        Short-lived bearer token for Phase 2 identity verification.
    mfa_token : str | None
        One-time password required for CRITICAL operations (Phase 3).
    rag_hint : str | None
        Reserved: context injected by a future RAG retrieval pipeline.
    trace_id : str
        Auto-generated UUIDv4 uniquely identifying this task execution.
    created_at : float
        Monotonic timestamp used to compute elapsed time for SLA tracking.
    """

    operation: str
    payload: Dict[str, Any] = field(default_factory=dict)
    operator_id: str = "system"
    session_token: Optional[str] = None
    mfa_token: Optional[str] = None
    rag_hint: Optional[str] = None
    trace_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_at: float = field(default_factory=time.monotonic)

    def elapsed_ms(self) -> float:
        """Return wall-time elapsed since task creation in milliseconds."""
        return (time.monotonic() - self.created_at) * 1000


# ---------------------------------------------------------------------------
# S3LogUploader — Integrated with AWS Academy Learner Lab for cloud-native observability.
# ---------------------------------------------------------------------------


class S3LogUploader:
    """
    Async-compatible S3 log uploader for audit trail persistence.

    Integrated with AWS Academy Learner Lab for cloud-native observability.

    Uploads structured, TraceID-stamped log payloads to an S3 bucket after
    each batch execution completes.  This satisfies the cloud-phone fleet
    management requirement of durable, queryable audit logs that survive
    process restarts — critical when operating 10,000+ devices and needing
    post-incident forensics across multiple batch windows.

    AWS credentials are read exclusively from environment variables to
    support the **temporary session tokens** issued by AWS Academy Learner
    Lab.  No credentials are ever hard-coded or committed to version control.

    Environment variables
    ---------------------
    AWS_ACCESS_KEY_ID       : Learner Lab access key
    AWS_SECRET_ACCESS_KEY   : Learner Lab secret key
    AWS_SESSION_TOKEN       : Learner Lab session token (required)
    AWS_DEFAULT_REGION      : AWS region (defaults to ``us-east-1``)
    S3_LOG_BUCKET           : Target S3 bucket name (required)
    S3_LOG_PREFIX           : Key prefix inside the bucket (default ``logs/``)

    Parameters
    ----------
    bucket : str | None
        Override bucket name (falls back to ``S3_LOG_BUCKET`` env var).
    prefix : str | None
        Override key prefix (falls back to ``S3_LOG_PREFIX`` env var).
    """

    def __init__(
        self,
        bucket: Optional[str] = None,
        prefix: Optional[str] = None,
    ) -> None:
        self._bucket = bucket or os.getenv("S3_LOG_BUCKET", "")
        self._prefix = prefix or os.getenv("S3_LOG_PREFIX", "logs/")
        self._region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
        self._enabled = bool(self._bucket)
        self._client: Optional[Any] = None

    def _get_client(self) -> Any:
        """
        Lazily construct the boto3 S3 client using Learner Lab credentials.

        Credentials are sourced from ``os.getenv`` at call time so that a
        token refresh (e.g. re-running the Learner Lab ``Start Lab`` flow)
        is picked up without restarting the engine process.
        """
        # Integrated with AWS Academy Learner Lab for cloud-native observability.
        return boto3.client(
            "s3",
            region_name=self._region,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        )

    async def upload_logs_to_s3(
        self,
        batch_id: str,
        results: List[Dict[str, Any]],
        *,
        extra_metadata: Optional[Dict[str, Any]] = None,
    ) -> Optional[str]:
        """
        Upload a batch execution log to S3 as a JSON document.

        Integrated with AWS Academy Learner Lab for cloud-native observability.

        The object key follows the pattern::

            {prefix}{date}/{batch_id}.json

        This partitioning by date enables efficient Athena / S3 Select queries
        over historical batch logs without full-bucket scans.

        Parameters
        ----------
        batch_id : str
            Short identifier for this batch execution (8-char UUID prefix).
        results : list[dict]
            The per-task result records produced by ``execute_batch``.
        extra_metadata : dict | None
            Optional metadata merged into the top-level JSON envelope
            (e.g. Prometheus snapshot, operator context).

        Returns
        -------
        str | None
            The full S3 key on success, or None if upload is disabled / failed.
        """
        if not self._enabled:
            logger.info(
                "S3LogUploader | upload skipped — S3_LOG_BUCKET not configured.",
                extra={"trace_id": batch_id},
            )
            return None

        now = datetime.now(timezone.utc)
        date_partition = now.strftime("%Y/%m/%d")
        key = f"{self._prefix}{date_partition}/{batch_id}.json"

        payload = {
            "batch_id": batch_id,
            "uploaded_at": now.isoformat(),
            "task_count": len(results),
            "results": results,
        }
        if extra_metadata:
            payload["metadata"] = extra_metadata

        body = json.dumps(payload, indent=2, default=str)

        loop = asyncio.get_running_loop()
        try:
            await loop.run_in_executor(None, self._put_object, key, body)
            logger.info(
                "S3LogUploader | uploaded s3://%s/%s (%d bytes)",
                self._bucket,
                key,
                len(body),
                extra={"trace_id": batch_id},
            )
            return key
        except (BotoCoreError, ClientError) as exc:
            logger.error(
                "S3LogUploader | upload failed for batch=%s: %s",
                batch_id,
                exc,
                extra={"trace_id": batch_id},
            )
            return None

    def _put_object(self, key: str, body: str) -> None:
        """Blocking S3 PutObject — executed in a thread-pool via run_in_executor."""
        client = self._get_client()
        client.put_object(
            Bucket=self._bucket,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/json",
        )


# ---------------------------------------------------------------------------
# ExecutionManager — the central async orchestrator
# ---------------------------------------------------------------------------


class ExecutionManager:
    """
    Async Industrial Execution Engine for cloud-ops-ai-agent.

    Refactored based on Baidu Cloud-Phone scalable architecture patterns.

    Responsibilities
    ----------------
    * Dispatch single and batch operations with full async lifecycle management.
    * Enforce SafetyGateway rules (3-phase) before any state-changing operation.
    * Honour CancellationToken checkpoints throughout long-running tasks.
    * Apply ``@handle_cloud_exceptions`` retry semantics on every op handler.
    * Emit structured, TraceID-stamped audit logs for every significant event.
    * Bound concurrent batch tasks via an asyncio.Semaphore sized by config.

    Parameters
    ----------
    config : AppConfig
        Runtime configuration loaded from config.json.
    gateway : SafetyGateway | None
        Optionally inject a custom gateway (useful for unit testing with mocks).
    """

    def __init__(
        self,
        config: AppConfig,
        gateway: Optional[SafetyGateway] = None,
        metrics: Optional[MetricsRegistry] = None,
        s3_uploader: Optional[S3LogUploader] = None,
    ) -> None:
        self._config = config
        self._gateway = gateway or SafetyGateway(config)
        self._metrics = metrics or MetricsRegistry.get()
        self._s3_uploader = s3_uploader or S3LogUploader()
        self._semaphore = asyncio.Semaphore(config.execution.batch_concurrency_limit)
        self._active_tokens: Dict[str, CancellationToken] = {}

        logger.setLevel(getattr(logging, config.log_level.upper(), logging.INFO))
        logger.info(
            "ExecutionManager initialised | app='%s' concurrency_limit=%d",
            config.app_name,
            config.execution.batch_concurrency_limit,
            extra={"trace_id": "BOOT"},
        )

    # ------------------------------------------------------------------
    # Public orchestration API
    # ------------------------------------------------------------------

    async def execute(self, ctx: TaskContext) -> Any:
        """
        Execute a single operation described by *ctx*.

        Pipeline
        --------
        1. Register a CancellationToken keyed by ``ctx.trace_id``.
        2. Run SafetyGateway 3-phase check.
        3. Dispatch to the mapped operation handler.
        4. Emit timing and audit log on completion or failure.
        5. Clean up the cancellation token.

        Returns
        -------
        Any
            The result produced by the operation handler.

        Raises
        ------
        TaskCancelledError
            If the operator cancelled the task mid-flight.
        CloudOpsError
            On any unrecoverable or exhausted-retry failure.
        """
        tlog = _trace_logger(ctx.trace_id)
        token = CancellationToken(ctx.trace_id)
        self._active_tokens[ctx.trace_id] = token

        tlog.info(
            "TASK START | op='%s' operator='%s' payload_keys=%s",
            ctx.operation,
            ctx.operator_id,
            list(ctx.payload.keys()),
        )
        await self._metrics.record_task_start()

        try:
            await self._gateway.check_operation(ctx.operation, ctx)
            result = await self._dispatch(ctx, token)

            elapsed = ctx.elapsed_ms()
            await self._metrics.record_task_success(ctx.operation, elapsed)
            tlog.info(
                "TASK DONE  | op='%s' elapsed_ms=%.1f",
                ctx.operation,
                elapsed,
            )
            return result

        except TaskCancelledError:
            elapsed = ctx.elapsed_ms()
            await self._metrics.record_task_cancelled(ctx.operation, elapsed)
            tlog.warning(
                "TASK CANCELLED | op='%s' elapsed_ms=%.1f reason='%s'",
                ctx.operation,
                elapsed,
                token.reason,
            )
            raise

        except SecurityViolationError:
            elapsed = ctx.elapsed_ms()
            risk = self._config.get_risk_level(ctx.operation) or "unknown"
            await self._metrics.record_safety_interception(ctx.operation, risk)
            await self._metrics.record_task_failure(ctx.operation, elapsed)
            tlog.error(
                "TASK INTERCEPTED | op='%s' risk='%s' elapsed_ms=%.1f",
                ctx.operation,
                risk,
                elapsed,
            )
            raise

        except CloudOpsError as exc:
            elapsed = ctx.elapsed_ms()
            await self._metrics.record_task_failure(ctx.operation, elapsed)
            tlog.error(
                "TASK FAILED | op='%s' error='%s' elapsed_ms=%.1f",
                ctx.operation,
                exc,
                elapsed,
            )
            raise

        finally:
            self._active_tokens.pop(ctx.trace_id, None)

    async def execute_batch(
        self,
        contexts: Sequence[TaskContext],
        *,
        fail_fast: bool = False,
    ) -> List[Dict[str, Any]]:
        """
        Execute a batch of operations with bounded concurrency.

        Modelled on the Cloud-Phone multi-device batch-management model: up to
        ``batch_concurrency_limit`` tasks run in parallel; the asyncio.Semaphore
        prevents resource exhaustion on the orchestration host.

        Parameters
        ----------
        contexts : Sequence[TaskContext]
            One TaskContext per operation to execute.
        fail_fast : bool
            When True, skip remaining queued tasks upon the first failure.

        Returns
        -------
        List[dict]
            One result record per input context, preserving input order.
            Each record contains ``trace_id``, ``status``, and either
            ``result`` or ``error``.
        """
        batch_id = str(uuid.uuid4())[:8]
        tlog = _trace_logger(batch_id)
        tlog.info("BATCH START | total=%d fail_fast=%s", len(contexts), fail_fast)

        results: List[Dict[str, Any]] = [{}] * len(contexts)
        abort_event = asyncio.Event()

        async def _run_one(idx: int, ctx: TaskContext) -> None:
            async with self._semaphore:
                if abort_event.is_set():
                    results[idx] = {
                        "trace_id": ctx.trace_id,
                        "status": "skipped",
                        "error": "batch aborted by fail_fast",
                    }
                    return
                try:
                    outcome = await self.execute(ctx)
                    results[idx] = {
                        "trace_id": ctx.trace_id,
                        "status": "success",
                        "result": outcome,
                    }
                except Exception as exc:  # noqa: BLE001
                    results[idx] = {
                        "trace_id": ctx.trace_id,
                        "status": "error",
                        "error": str(exc),
                    }
                    if fail_fast:
                        tlog.error(
                            "BATCH fail_fast triggered | trace=%s error='%s'",
                            ctx.trace_id,
                            exc,
                        )
                        abort_event.set()

        await asyncio.gather(*(_run_one(i, c) for i, c in enumerate(contexts)))

        success = sum(1 for r in results if r.get("status") == "success")
        tlog.info(
            "BATCH DONE | total=%d success=%d failed/skipped=%d",
            len(contexts),
            success,
            len(contexts) - success,
        )

        # Integrated with AWS Academy Learner Lab for cloud-native observability.
        # Auto-upload batch audit logs to S3 with TraceID-keyed partitioning.
        await self._s3_uploader.upload_logs_to_s3(
            batch_id=batch_id,
            results=results,
            extra_metadata={
                "total": len(contexts),
                "success": success,
                "failed_or_skipped": len(contexts) - success,
                "fail_fast": fail_fast,
            },
        )

        return results

    def cancel_task(self, trace_id: str, reason: str = "operator cancelled") -> bool:
        """
        Cooperatively cancel a running task by its *trace_id*.

        The task is interrupted at its next ``raise_if_cancelled()`` checkpoint.
        No forced termination is performed — this is the graceful-abort model
        from Baidu's Cloud-Phone task management console, where operators
        confirm before any irreversible action is taken.

        Returns
        -------
        bool
            True if the token was found and signalled; False if unknown trace_id.
        """
        token = self._active_tokens.get(trace_id)
        if token:
            token.cancel(reason=reason)
            return True
        logger.warning(
            "cancel_task: no active task with trace_id='%s'",
            trace_id,
            extra={"trace_id": "N/A"},
        )
        return False

    # ------------------------------------------------------------------
    # Operation dispatcher
    # ------------------------------------------------------------------

    async def _dispatch(self, ctx: TaskContext, token: CancellationToken) -> Any:
        """
        Route *ctx.operation* to the appropriate async handler.

        New operations can be registered here without touching any other
        part of the engine — facilitating modular RAG-augmented command
        discovery in future iterations.
        """
        handlers: Dict[str, Callable[..., Awaitable[Any]]] = {
            "delete_node": self._op_delete_node,
            "restart_node": self._op_restart_node,
            "batch_update_devices": self._op_batch_update_devices,
            "health_check": self._op_health_check,
            "collect_metrics": self._op_collect_metrics,
        }
        handler = handlers.get(ctx.operation, self._op_generic)
        return await handler(ctx, token)

    # ------------------------------------------------------------------
    # Concrete operation handlers
    # ------------------------------------------------------------------

    @handle_cloud_exceptions
    async def _op_delete_node(
        self, ctx: TaskContext, token: CancellationToken
    ) -> Dict[str, Any]:
        """
        CRITICAL: permanently remove a cloud-phone node from the cluster.

        Called only after the SafetyGateway has passed all three verification
        phases (risk classification, identity, MFA).
        """
        tlog = _trace_logger(ctx.trace_id)
        node_id = ctx.payload.get("node_id", "unknown")
        tlog.info("Deleting node '%s' …", node_id)

        token.raise_if_cancelled()
        await asyncio.sleep(0.2)  # simulated teardown I/O
        token.raise_if_cancelled()

        tlog.info("Node '%s' deleted successfully.", node_id)
        return {"deleted": node_id, "status": "ok"}

    @handle_cloud_exceptions
    async def _op_restart_node(
        self, ctx: TaskContext, token: CancellationToken
    ) -> Dict[str, Any]:
        """HIGH: gracefully drain, stop, and restart a cloud-phone node."""
        tlog = _trace_logger(ctx.trace_id)
        node_id = ctx.payload.get("node_id", "unknown")
        tlog.info("Restarting node '%s' …", node_id)

        for phase in ("draining", "stopping", "starting"):
            token.raise_if_cancelled()
            tlog.info("  [restart] phase='%s' node='%s'", phase, node_id)
            await asyncio.sleep(0.1)

        return {"restarted": node_id, "status": "ok"}

    @handle_cloud_exceptions
    async def _op_batch_update_devices(
        self, ctx: TaskContext, token: CancellationToken
    ) -> Dict[str, Any]:
        """
        MEDIUM: push a configuration update to a list of cloud-phone devices.

        This is a long-running task.  ``raise_if_cancelled()`` is called before
        every device update so that the operator's abort request is honoured
        within one cancellation-check interval — never silently swallowed.
        """
        tlog = _trace_logger(ctx.trace_id)
        device_ids: List[str] = ctx.payload.get("device_ids", [])
        interval = self._config.execution.cancellation_check_interval_seconds

        tlog.info("Batch-updating %d devices …", len(device_ids))
        updated: List[str] = []

        for device_id in device_ids:
            token.raise_if_cancelled()  # high-frequency cancellation_point

            tlog.info("  Updating device '%s' …", device_id)
            await asyncio.sleep(interval)  # simulated per-device API call
            updated.append(device_id)

        tlog.info(
            "Batch update complete — %d/%d devices updated.",
            len(updated),
            len(device_ids),
        )
        return {"updated": updated, "total": len(device_ids)}

    @handle_cloud_exceptions
    async def _op_health_check(
        self, ctx: TaskContext, token: CancellationToken
    ) -> Dict[str, Any]:
        """LOW: retrieve a cluster health snapshot — read-only, no verification needed."""
        tlog = _trace_logger(ctx.trace_id)
        tlog.info("Running health check …")
        token.raise_if_cancelled()
        await asyncio.sleep(0.05)
        return {"healthy": True, "latency_ms": round(ctx.elapsed_ms(), 2)}

    @handle_cloud_exceptions
    async def _op_collect_metrics(
        self, ctx: TaskContext, token: CancellationToken
    ) -> Dict[str, Any]:
        """LOW: aggregate performance metrics across the device fleet."""
        tlog = _trace_logger(ctx.trace_id)
        tlog.info("Collecting metrics …")
        token.raise_if_cancelled()
        await asyncio.sleep(0.08)
        return {"cpu_avg_pct": 42.1, "mem_avg_mb": 1024, "active_devices": 128}

    @handle_cloud_exceptions
    async def _op_generic(
        self, ctx: TaskContext, token: CancellationToken
    ) -> Dict[str, Any]:
        """
        Fallback handler for operations not explicitly mapped in ``_dispatch``.

        Simulates a remote API call with a timeout and cancellation checkpoints,
        making it safe to add new operation types without extra scaffolding.
        This generic handler is also a convenient hook for RAG-guided dynamic
        operation execution.
        """
        tlog = _trace_logger(ctx.trace_id)
        timeout = self._config.execution.default_timeout_seconds
        tlog.info(
            "Generic handler | op='%s' timeout=%.1fs", ctx.operation, timeout
        )

        async def _work() -> Dict[str, Any]:
            token.raise_if_cancelled()
            await asyncio.sleep(0.1)
            token.raise_if_cancelled()
            return {"operation": ctx.operation, "status": "completed"}

        return await asyncio.wait_for(_work(), timeout=timeout)


# ---------------------------------------------------------------------------
# Module-level factory — preferred entry point for external callers
# ---------------------------------------------------------------------------


def create_engine(config_path: Optional[str] = None) -> ExecutionManager:
    """
    Instantiate a fully configured ExecutionManager.

    This factory is the recommended entry point for application code and
    for the future RAG integration layer — it keeps construction logic in
    a single, discoverable location.

    Parameters
    ----------
    config_path : str | None
        Override path to config.json.  Defaults to the co-located file.

    Returns
    -------
    ExecutionManager
    """
    cfg = AppConfig(config_path)
    return ExecutionManager(cfg)


# ---------------------------------------------------------------------------
# Minimal smoke-test — run directly to validate the full pipeline end-to-end
# ---------------------------------------------------------------------------


async def _smoke_test() -> None:
    """
    Quick end-to-end validation of the execution engine.

    Not a substitute for unit tests — intended for rapid manual verification
    during development or CI smoke stages.
    """
    engine = create_engine()

    print("\n=== health_check (LOW risk) ===")
    result = await engine.execute(
        TaskContext(
            operation="health_check",
            operator_id="demo-user",
            session_token="valid-token",
        )
    )
    print("Result:", result)

    print("\n=== restart_node (HIGH risk — identity verification required) ===")
    result = await engine.execute(
        TaskContext(
            operation="restart_node",
            payload={"node_id": "node-007"},
            operator_id="sre-alice",
            session_token="valid-token-alice",
        )
    )
    print("Result:", result)

    print("\n=== batch_update_devices with mid-flight cooperative cancel ===")
    ctx_batch = TaskContext(
        operation="batch_update_devices",
        payload={"device_ids": [f"dev-{i:03d}" for i in range(8)]},
        operator_id="system",
        session_token="valid-token-sys",
    )

    async def _cancel_after(trace_id: str, delay: float) -> None:
        await asyncio.sleep(delay)
        engine.cancel_task(trace_id, reason="smoke-test early abort")

    asyncio.create_task(_cancel_after(ctx_batch.trace_id, 0.9))
    try:
        await engine.execute(ctx_batch)
    except TaskCancelledError as exc:
        print("Gracefully cancelled:", exc)

    print("\n=== delete_node (CRITICAL — identity + MFA required) ===")
    result = await engine.execute(
        TaskContext(
            operation="delete_node",
            payload={"node_id": "node-doom"},
            operator_id="admin-bob",
            session_token="valid-token-admin",
            mfa_token="123456",
        )
    )
    print("Result:", result)

    print("\n=== execute_batch — two parallel LOW-risk tasks ===")
    batch_results = await engine.execute_batch(
        [
            TaskContext(
                operation="health_check",
                operator_id="batch-runner",
                session_token="s1",
            ),
            TaskContext(
                operation="collect_metrics",
                operator_id="batch-runner",
                session_token="s2",
            ),
        ]
    )
    for r in batch_results:
        print(" ", r)

    print("\n=== Prometheus Metrics Snapshot ===")
    print(MetricsRegistry.get().to_prometheus_format())


if __name__ == "__main__":
    asyncio.run(_smoke_test())
