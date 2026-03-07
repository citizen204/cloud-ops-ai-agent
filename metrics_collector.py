"""
metrics_collector.py — Async Metrics Registry for cloud-ops-ai-agent.

Refactored based on Baidu Cloud-Phone scalable architecture patterns.

This module provides a process-wide, async-safe metrics registry modelled
on the Prometheus data model.  It was designed to satisfy the **system health
modelling** requirements encountered when operating 10,000+ concurrent
cloud-phone instances on Baidu's fleet management platform, where real-time
visibility into task throughput, failure rates, and safety-gate hit rates
is essential for SRE decision-making.

Design decisions
----------------
* **Async singleton** — ``MetricsRegistry.get()`` returns the one global
  instance; ``asyncio.Lock`` guards every mutation so counters stay
  consistent under high-concurrency batch workloads.
* **Prometheus text exposition** — ``to_prometheus_format()`` emits the
  standard ``# HELP / # TYPE / metric_name{labels} value`` format,
  ready to be scraped by a ``/metrics`` HTTP endpoint or pushed to
  Pushgateway.
* **Per-operation labels** — every counter and histogram is keyed by
  ``operation``, matching the label cardinality patterns recommended by
  Prometheus best practices.
* **Zero external dependencies** — the module uses only the standard
  library so it can be imported in any deployment environment.

RAG integration note
---------------------
All public methods carry typed signatures and docstrings so that a future
RAG pipeline can surface health-modelling procedures directly from this
module's documentation.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

_LOG_FORMAT = (
    "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)

_logger = logging.getLogger("metrics_collector")
if not _logger.handlers:
    _h = logging.StreamHandler()
    _h.setFormatter(logging.Formatter(_LOG_FORMAT))
    _logger.addHandler(_h)
_logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Internal metric types
# ---------------------------------------------------------------------------


@dataclass
class _CounterState:
    """Atomic counter keyed by (metric_name, label_set)."""

    value: int = 0


@dataclass
class _HistogramState:
    """
    Lightweight histogram tracking sum, count, min, max.

    Sufficient for computing averages and detecting outliers without
    the memory overhead of full bucket boundaries.
    """

    count: int = 0
    total: float = 0.0
    min_val: float = float("inf")
    max_val: float = float("-inf")

    @property
    def avg(self) -> float:
        return self.total / self.count if self.count else 0.0

    def observe(self, value: float) -> None:
        self.count += 1
        self.total += value
        if value < self.min_val:
            self.min_val = value
        if value > self.max_val:
            self.max_val = value


# ---------------------------------------------------------------------------
# MetricsRegistry — async-safe global singleton
# ---------------------------------------------------------------------------


class MetricsRegistry:
    """
    Process-wide async metrics registry for cloud-ops-ai-agent.

    Designed for the system-health-modelling requirements of large-scale
    cloud-phone fleet management: real-time task throughput, failure rate,
    safety-gate interception rate, and per-operation latency distribution.

    This class implements the **singleton pattern** — call ``get()`` to
    obtain the one shared instance, or ``create()`` to explicitly
    (re-)initialise it (useful in test fixtures).

    Metric families
    ---------------
    ``cloudops_tasks_total``
        Counter.  Labels: ``operation``, ``status`` (success | failed | cancelled).

    ``cloudops_safety_interceptions_total``
        Counter.  Labels: ``operation``, ``risk_level``.
        Incremented every time a SafetyGateway phase rejects an operation.

    ``cloudops_task_duration_seconds``
        Histogram (sum/count/min/max).  Labels: ``operation``.
        Records wall-clock duration for every completed or failed task.

    ``cloudops_active_tasks``
        Gauge.  Labels: none (global).  Tracks in-flight task count.

    Thread / coroutine safety
    -------------------------
    All mutations acquire an ``asyncio.Lock`` before touching internal
    state, so concurrent ``execute()`` coroutines never produce torn
    reads or lost increments.

    Examples
    --------
    ::

        registry = MetricsRegistry.get()
        await registry.record_task_success("health_check", elapsed_ms=42.3)
        print(registry.to_prometheus_format())
    """

    _instance: Optional[MetricsRegistry] = None

    def __init__(self) -> None:
        self._lock = asyncio.Lock()

        self._task_counters: Dict[str, _CounterState] = defaultdict(_CounterState)
        self._safety_counters: Dict[str, _CounterState] = defaultdict(_CounterState)

        self._duration_histograms: Dict[str, _HistogramState] = defaultdict(
            _HistogramState
        )

        self._active_tasks: int = 0
        self._created_at: float = time.monotonic()

        _logger.info("MetricsRegistry initialised.")

    # ------------------------------------------------------------------
    # Singleton lifecycle
    # ------------------------------------------------------------------

    @classmethod
    def get(cls) -> MetricsRegistry:
        """Return the global singleton, creating it on first access."""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    @classmethod
    def create(cls) -> MetricsRegistry:
        """Force-create a fresh registry (resets all counters)."""
        cls._instance = cls()
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Destroy the singleton — next ``get()`` will create a new one."""
        cls._instance = None

    # ------------------------------------------------------------------
    # Recording API — called by ExecutionManager
    # ------------------------------------------------------------------

    async def record_task_start(self) -> None:
        """Increment the in-flight task gauge."""
        async with self._lock:
            self._active_tasks += 1

    async def record_task_success(
        self, operation: str, elapsed_ms: float
    ) -> None:
        """
        Record a successfully completed task.

        Parameters
        ----------
        operation : str
            The logical operation name (e.g. ``"health_check"``).
        elapsed_ms : float
            Wall-clock execution time in milliseconds.
        """
        async with self._lock:
            self._active_tasks = max(0, self._active_tasks - 1)
            key = f'{operation}|success'
            self._task_counters[key].value += 1
            self._duration_histograms[operation].observe(elapsed_ms / 1000.0)

    async def record_task_failure(
        self, operation: str, elapsed_ms: float
    ) -> None:
        """Record a task that ended in an unrecoverable error."""
        async with self._lock:
            self._active_tasks = max(0, self._active_tasks - 1)
            key = f'{operation}|failed'
            self._task_counters[key].value += 1
            self._duration_histograms[operation].observe(elapsed_ms / 1000.0)

    async def record_task_cancelled(
        self, operation: str, elapsed_ms: float
    ) -> None:
        """Record a task that was cooperatively cancelled."""
        async with self._lock:
            self._active_tasks = max(0, self._active_tasks - 1)
            key = f'{operation}|cancelled'
            self._task_counters[key].value += 1
            self._duration_histograms[operation].observe(elapsed_ms / 1000.0)

    async def record_safety_interception(
        self, operation: str, risk_level: str
    ) -> None:
        """
        Record a SafetyGateway interception (identity or MFA rejection).

        Parameters
        ----------
        operation : str
            The operation that was blocked.
        risk_level : str
            Risk level at which the interception occurred (e.g. ``"critical"``).
        """
        async with self._lock:
            key = f'{operation}|{risk_level}'
            self._safety_counters[key].value += 1

    # ------------------------------------------------------------------
    # Query API
    # ------------------------------------------------------------------

    async def snapshot(self) -> Dict[str, Any]:
        """
        Return a point-in-time dictionary snapshot of all metrics.

        Useful for JSON API responses or internal dashboards.
        """
        async with self._lock:
            tasks: Dict[str, int] = {}
            for key, counter in self._task_counters.items():
                tasks[key] = counter.value

            safety: Dict[str, int] = {}
            for key, counter in self._safety_counters.items():
                safety[key] = counter.value

            durations: Dict[str, Dict[str, float]] = {}
            for op, hist in self._duration_histograms.items():
                durations[op] = {
                    "count": hist.count,
                    "total_seconds": round(hist.total, 6),
                    "avg_seconds": round(hist.avg, 6),
                    "min_seconds": round(hist.min_val, 6)
                    if hist.min_val != float("inf")
                    else 0.0,
                    "max_seconds": round(hist.max_val, 6)
                    if hist.max_val != float("-inf")
                    else 0.0,
                }

            return {
                "active_tasks": self._active_tasks,
                "task_counters": dict(tasks),
                "safety_interceptions": dict(safety),
                "duration_histograms": durations,
                "uptime_seconds": round(
                    time.monotonic() - self._created_at, 3
                ),
            }

    # ------------------------------------------------------------------
    # Prometheus text exposition
    # ------------------------------------------------------------------

    def to_prometheus_format(self) -> str:
        """
        Render all metrics in Prometheus text exposition format.

        Output is suitable for direct use as an HTTP ``/metrics`` response
        body with ``Content-Type: text/plain; version=0.0.4; charset=utf-8``.

        Metric names follow the ``cloudops_`` namespace convention to avoid
        collisions when co-deployed with other exporters.

        Returns
        -------
        str
            Multi-line Prometheus-compatible metrics text.
        """
        lines: List[str] = []

        # -- cloudops_tasks_total (counter) ---
        lines.append(
            "# HELP cloudops_tasks_total "
            "Total number of tasks processed by the execution engine."
        )
        lines.append("# TYPE cloudops_tasks_total counter")
        for key, counter in sorted(self._task_counters.items()):
            operation, status = key.split("|", 1)
            lines.append(
                f'cloudops_tasks_total{{operation="{operation}",'
                f'status="{status}"}} {counter.value}'
            )

        lines.append("")

        # -- cloudops_safety_interceptions_total (counter) ---
        lines.append(
            "# HELP cloudops_safety_interceptions_total "
            "Total SafetyGateway interceptions (identity/MFA rejections)."
        )
        lines.append("# TYPE cloudops_safety_interceptions_total counter")
        for key, counter in sorted(self._safety_counters.items()):
            operation, risk_level = key.split("|", 1)
            lines.append(
                f'cloudops_safety_interceptions_total'
                f'{{operation="{operation}",'
                f'risk_level="{risk_level}"}} {counter.value}'
            )

        lines.append("")

        # -- cloudops_task_duration_seconds (summary-style) ---
        lines.append(
            "# HELP cloudops_task_duration_seconds "
            "Task execution duration in seconds."
        )
        lines.append("# TYPE cloudops_task_duration_seconds summary")
        for op, hist in sorted(self._duration_histograms.items()):
            lines.append(
                f'cloudops_task_duration_seconds_sum'
                f'{{operation="{op}"}} {hist.total:.6f}'
            )
            lines.append(
                f'cloudops_task_duration_seconds_count'
                f'{{operation="{op}"}} {hist.count}'
            )
            if hist.count > 0:
                lines.append(
                    f'cloudops_task_duration_seconds_avg'
                    f'{{operation="{op}"}} {hist.avg:.6f}'
                )
                lines.append(
                    f'cloudops_task_duration_seconds_min'
                    f'{{operation="{op}"}} {hist.min_val:.6f}'
                )
                lines.append(
                    f'cloudops_task_duration_seconds_max'
                    f'{{operation="{op}"}} {hist.max_val:.6f}'
                )

        lines.append("")

        # -- cloudops_active_tasks (gauge) ---
        lines.append(
            "# HELP cloudops_active_tasks "
            "Number of tasks currently in-flight."
        )
        lines.append("# TYPE cloudops_active_tasks gauge")
        lines.append(f"cloudops_active_tasks {self._active_tasks}")

        lines.append("")

        # -- cloudops_uptime_seconds (gauge) ---
        uptime = time.monotonic() - self._created_at
        lines.append(
            "# HELP cloudops_uptime_seconds "
            "Seconds since MetricsRegistry was initialised."
        )
        lines.append("# TYPE cloudops_uptime_seconds gauge")
        lines.append(f"cloudops_uptime_seconds {uptime:.3f}")

        lines.append("")
        return "\n".join(lines)
