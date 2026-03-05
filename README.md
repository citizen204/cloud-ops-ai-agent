# cloud-ops-ai-agent

<div align="center">

[![Python](https://img.shields.io/badge/Python-3.11%2B-3776AB?style=for-the-badge&logo=python&logoColor=white)](https://www.python.org/)
[![Asyncio](https://img.shields.io/badge/Asyncio-Native-00B4D8?style=for-the-badge&logo=python&logoColor=white)](https://docs.python.org/3/library/asyncio.html)
[![Architecture](https://img.shields.io/badge/Architecture-Event--Driven-6C3483?style=for-the-badge&logo=apacheairflow&logoColor=white)]()
[![Safety](https://img.shields.io/badge/Safety-3--Phase%20Gateway-E74C3C?style=for-the-badge&logo=shield&logoColor=white)]()
[![Config](https://img.shields.io/badge/Config-Zero%20Hardcoding-27AE60?style=for-the-badge&logo=json&logoColor=white)]()
[![Observability](https://img.shields.io/badge/Observability-TraceID%20%7C%20ELK%20Ready-F39C12?style=for-the-badge&logo=elastic&logoColor=white)]()
[![RAG](https://img.shields.io/badge/RAG-Integration%20Ready-8E44AD?style=for-the-badge&logo=openai&logoColor=white)]()
[![License](https://img.shields.io/badge/License-MIT-lightgrey?style=for-the-badge)]()

<br/>

> **An experimental framework merging large-scale cloud-phone operational experience from Baidu with modern async AI agent design patterns.**

</div>

---

## Project Vision

`cloud-ops-ai-agent` is not just a task runner — it is an **industrial-grade async execution engine** designed around the operational realities of managing thousands of concurrent virtual devices at scale.

The architecture was directly informed by real-world engineering challenges encountered during an internship on **Baidu's Cloud-Phone (红手指) team**: How do you safely orchestrate destructive operations across a massive device fleet? How do you let an operator abort a rolling update mid-flight without data corruption? How do you make every configuration parameter tunable from a backend dashboard without a single code deploy?

This project fuses those hard-won lessons with modern Python asyncio primitives, a three-phase cryptographic safety gateway, and a RAG-ready documentation contract — producing a framework that is ready for both **production cloud operations** and **AI-augmented runbook automation**.

```
Baidu Cloud-Phone Scale Ops  ──►  Async Python Engine  ──►  AI Agent Interface
   (10,000+ devices)               (asyncio + Safety)        (RAG-augmented)
```

---

## Key Architectural Features

### 1. Bounded Concurrency via `asyncio.Semaphore`

Managing a fleet of virtual phones means launching dozens of operations simultaneously. Unbounded parallelism saturates the orchestration host and causes cascading failures. The engine solves this with a **config-driven semaphore** that creates a precise ceiling on concurrent task slots.

```python
# Concurrency limit sourced exclusively from config.json — never hard-coded
self._semaphore = asyncio.Semaphore(config.execution.batch_concurrency_limit)

async def _run_one(idx: int, ctx: TaskContext) -> None:
    async with self._semaphore:   # blocks here if the slot pool is full
        outcome = await self.execute(ctx)
```

When `batch_concurrency_limit = 10` (as configured), the 11th device operation queues transparently behind an active slot. This ensures:

- **No thread-pool exhaustion** — all I/O is non-blocking within the event loop.
- **Predictable memory footprint** — at most N tasks hold open connections simultaneously.
- **Back-pressure propagation** — slowdowns in the target API naturally throttle the batch rate.

```mermaid
flowchart LR
    subgraph Batch["execute_batch()"]
        direction TB
        D0["TaskContext[0]"]
        D1["TaskContext[1]"]
        D2["TaskContext[2]"]
        DN["TaskContext[N]"]
    end

    subgraph Semaphore["asyncio.Semaphore(limit=10)"]
        S1["Slot 1"]
        S2["Slot 2"]
        S3["Slot 3"]
        SD["..."]
        S10["Slot 10"]
    end

    subgraph Engine["ExecutionManager.execute()"]
        GW["SafetyGateway"]
        DISP["_dispatch()"]
        HANDLER["Op Handler"]
    end

    D0 --> S1 --> GW
    D1 --> S2 --> GW
    D2 --> S3 --> GW
    DN -->|queued| SD
    GW --> DISP --> HANDLER
```

---

### 2. Three-Phase Safety Gateway

The `SafetyGateway` is the security backbone of the engine, directly modelled on the **account-security and verification mechanisms** designed during the Baidu Cloud-Phone internship. Before any state-changing operation reaches an execution handler, it must pass all applicable phases in strict order.

```mermaid
sequenceDiagram
    actor Operator
    participant EM as ExecutionManager
    participant GW as SafetyGateway
    participant IdP as Identity Provider
    participant TOTP as MFA Service
    participant Handler as Op Handler

    Operator->>EM: execute(TaskContext{op="delete_node", mfa_token="..."})
    EM->>GW: check_operation("delete_node", ctx)

    Note over GW: Phase 1 — Risk Classification
    GW->>GW: config.get_risk_profile("delete_node") → CRITICAL

    Note over GW: Phase 2 — Identity Verification
    GW->>IdP: await _idp_verify(operator_id, session_token)
    IdP-->>GW: verified=True  (cached for session_token_ttl_seconds)

    Note over GW: Phase 3 — MFA Confirmation (CRITICAL only)
    GW->>TOTP: validate mfa_token
    TOTP-->>GW: confirmed=True

    Note over GW: Cooldown (10s for CRITICAL)
    GW-->>EM: all phases passed ✓

    EM->>Handler: _op_delete_node(ctx, token)
    Handler-->>EM: {"deleted": "node-doom", "status": "ok"}
    EM-->>Operator: result
```

| Phase | Trigger | Action on Failure |
|---|---|---|
| **1 — Risk Classification** | Every operation | Defaults to `HIGH`; raises `SecurityViolationError` if policy explicitly blocks |
| **2 — Identity Verification** | `HIGH` + `CRITICAL` ops | Raises `IdentityVerificationError`; op never executed |
| **3 — MFA Confirmation** | `CRITICAL` ops only | Raises `IdentityVerificationError` if `mfa_token` absent or invalid |

> A session-verification cache (`trace_id → verified_at`) prevents redundant IdP round-trips within the same task's TTL window, balancing security with performance under high-concurrency batch workloads.

---

### 3. Zero-Hardcoding Configuration (`AppConfig`)

Every numerical constant — retry limits, backoff delays, concurrency caps, operation risk classifications, RAG thresholds — lives in a single `config.json` and is parsed once at startup into **frozen, immutable dataclasses**.

```python
@dataclass(frozen=True)      # immutable after construction — prevents accidental mutation
class RetryConfig:
    max_attempts: int
    base_backoff_seconds: float
    max_backoff_seconds: float
    backoff_multiplier: float
    jitter_factor: float
```

`AppConfig` is constructed once and injected into every component. No component ever reads a file or accesses an environment variable directly — they receive a typed, validated configuration object through their constructor.

```mermaid
graph TD
    CF["config.json"] -->|parsed once| AC["AppConfig"]
    AC -->|".retry"| RC["RetryConfig (frozen)"]
    AC -->|".execution"| EC["ExecutionConfig (frozen)"]
    AC -->|".safety"| SC["SafetyConfig (frozen)"]
    AC -->|".risk_profiles"| RP["Dict[str, RiskProfile] (frozen)"]
    AC -->|".rag"| RAG["RagConfig (frozen)"]

    AC -->|injected| EM["ExecutionManager"]
    AC -->|injected| GW["SafetyGateway"]
    AC -->|read by| DEC["@handle_cloud_exceptions"]

    style CF fill:#FEF9C3,stroke:#D4AC0D
    style AC fill:#D5F5E3,stroke:#1E8449
    style EM fill:#D6EAF8,stroke:#1A5276
    style GW fill:#FADBD8,stroke:#922B21
```

To change the retry policy in production, an operator updates `config.json` and restarts the engine — **no code change, no redeploy of business logic**.

---

### 4. Cooperative Graceful Termination (`CancellationToken`)

Long-running batch operations — like rolling out a firmware update to 10,000 cloud-phone instances — must be stoppable mid-flight without leaving devices in an inconsistent state.

The engine implements this via `CancellationToken`, a cooperative abort mechanism backed by `asyncio.Event`. An operator signals cancellation through `ExecutionManager.cancel_task(trace_id)`; the running coroutine checks the signal at every logical checkpoint via `token.raise_if_cancelled()`.

```python
for device_id in device_ids:
    token.raise_if_cancelled()          # high-frequency cancellation_point

    tlog.info("  Updating device '%s' …", device_id)
    await asyncio.sleep(interval)       # non-blocking per-device API call
    updated.append(device_id)
```

There is **no `asyncio.Task.cancel()` involved** — the coroutine always reaches a clean checkpoint before raising `TaskCancelledError`. This mirrors the **task-abort confirmation UX** from Baidu's Cloud-Phone management console, where a single abort button gracefully drains the current sub-operation before halting.

```mermaid
stateDiagram-v2
    [*] --> RUNNING : execute(ctx)
    RUNNING --> CHECKPOINT : raise_if_cancelled()
    CHECKPOINT --> RUNNING : token.is_cancelled == False
    CHECKPOINT --> CANCELLED : token.is_cancelled == True
    CANCELLED --> [*] : raise TaskCancelledError\n(logged with trace_id + reason)
    RUNNING --> DONE : all devices updated
    DONE --> [*]
```

---

### 5. Industrial-Grade Retry: `@handle_cloud_exceptions`

A single decorator wraps every operation handler with full retry semantics, sourcing all parameters from `self._config.retry` at call time.

```
delay(attempt) = min(base × multiplier^(attempt-1), max_backoff) + jitter
```

Where `jitter = delay × jitter_factor × random()` — drawn uniformly per attempt to prevent retry storms across concurrent batch tasks (the **thundering-herd problem**).

| Exception Type | Behaviour |
|---|---|
| `TransientCloudError` | Retried up to `max_attempts` with exponential backoff |
| `PermanentCloudError` | Propagates immediately — never retried |
| `TaskCancelledError` | Propagates immediately — cooperative abort honoured |
| `asyncio.CancelledError` | Re-raised — event-loop contract preserved |
| Any other `Exception` | Wrapped in `PermanentCloudError` + logged with full traceback |

---

## Observability: TraceID Full-Chain Tracing

Every task is assigned a **UUIDv4 `trace_id`** at `TaskContext` creation time. This ID propagates through every log line emitted during that task's lifecycle — from gateway phases to retry attempts to final audit records.

```
2026-03-05 11:10:20,662 | INFO  | trace=f6540d51-5c00-4e2a-8820-71e5ef062e1c | SafetyGateway | Phase 1 complete | op='delete_node' risk='critical'
2026-03-05 11:10:20,714 | INFO  | trace=f6540d51-5c00-4e2a-8820-71e5ef062e1c | SafetyGateway | Phase 2 passed — identity verified.
2026-03-05 11:10:20,766 | INFO  | trace=f6540d51-5c00-4e2a-8820-71e5ef062e1c | SafetyGateway | Phase 3 passed — MFA confirmed.
2026-03-05 11:10:30,971 | INFO  | trace=f6540d51-5c00-4e2a-8820-71e5ef062e1c | Op            | Node 'node-doom' deleted successfully.
2026-03-05 11:10:30,971 | INFO  | trace=f6540d51-5c00-4e2a-8820-71e5ef062e1c | Engine        | TASK DONE | elapsed_ms=10309.1
```

A single `grep trace=f6540d51` in Kibana or Grafana Loki reconstructs the **entire operation timeline** — across gateway phases, retries, and the final result — with no additional instrumentation required.

```mermaid
graph LR
    subgraph Application
        TC["TaskContext\ntrace_id = uuid4()"]
        LOG["_trace_logger(trace_id)\nLoggerAdapter"]
        TC -->|stamped on| LOG
    end

    subgraph Transport
        STDOUT["stdout / stderr\n(structured one-liners)"]
        LOG --> STDOUT
    end

    subgraph Aggregation
        FB["Filebeat / Fluentd"]
        STDOUT --> FB
    end

    subgraph Query
        ES["Elasticsearch\n/ Loki"]
        KB["Kibana / Grafana\ngrep trace=<uuid>"]
        FB --> ES --> KB
    end

    style TC fill:#D6EAF8,stroke:#1A5276
    style KB fill:#D5F5E3,stroke:#1E8449
```

**Batch operations** use a secondary `batch_id` (8-char UUID prefix) so you can query either the batch-level summary or any individual task within it.

---

## How It Aligns with My Baidu Cloud-Phone Internship

The architectural decisions in this codebase map directly to engineering work delivered during the Baidu 红手指 (Cloud-Phone) internship:

| Code Module | Internship Deliverable | Evidence |
|---|---|---|
| `CancellationToken` + `raise_if_cancelled()` | **任务中止按钮及确认流程** — Designed the operator-facing abort UI and the backend cooperative termination contract that prevented partial device-state corruption during rolling updates | `_op_batch_update_devices` inserts a checkpoint before every device; abort is honoured within one `cancellation_check_interval_seconds` |
| `AppConfig` + `config.json` + `@dataclass(frozen=True)` | **功能说明可配置化 / 后台可配置方案** — Moved hardcoded operational parameters (timeouts, retry counts, risk thresholds) into a backend-editable configuration layer, enabling ops teams to tune behaviour without code deployments | All 5 sub-configs (`RetryConfig`, `ExecutionConfig`, `SafetyConfig`, `RiskProfile`, `RagConfig`) are frozen dataclasses sourced entirely from `config.json` |
| `SafetyGateway` (Phase 2 + Phase 3) | **账号安全体系与验证机制** — Implemented the identity-verification and MFA challenge flow that gates destructive account operations (session invalidation, privilege escalation) on the Cloud-Phone platform | Phase 2 calls `_idp_verify` with session-cache TTL; Phase 3 enforces `mfa_token` presence for `CRITICAL` risk ops; `IdentityVerificationError` is non-retryable |
| `@handle_cloud_exceptions` + `TransientCloudError` | **大规模并发环境下的鲁棒性** — Contributed to the retry and circuit-breaker layer that shielded upstream device APIs from thundering-herd retries during batch reboots | Exponential backoff with per-attempt jitter prevents correlated retries across concurrent batch tasks |
| `TraceID` + `_trace_logger` | **可追溯性 / 操作审计** — Established structured log correlation for multi-device operations, enabling post-incident tracing across thousands of simultaneous sessions | Every log line carries `trace=<uuid4>`, directly queryable in ELK without additional APM instrumentation |

---

## Complete System Architecture

```mermaid
flowchart TD
    subgraph External["External Callers / AI Agent / RAG Pipeline"]
        CLI["CLI / Agent Planner"]
        RAG["RAG Knowledge Base\n(future: rag_hint injection)"]
    end

    subgraph Factory["Module Entry Point"]
        CE["create_engine(config_path)"]
    end

    subgraph Config["Configuration Layer"]
        CF["config.json"] --> AC["AppConfig"]
        AC --> RC["RetryConfig"]
        AC --> EC["ExecutionConfig"]
        AC --> SC["SafetyConfig"]
        AC --> RP["RiskProfiles"]
        AC --> RAGC["RagConfig"]
    end

    subgraph Core["Execution Engine"]
        EM["ExecutionManager\n(orchestrator)"]
        SEM["asyncio.Semaphore\n(bounded concurrency)"]
        TOKEN["CancellationToken\n(asyncio.Event)"]

        subgraph Gateway["SafetyGateway"]
            P1["Phase 1\nRisk Classification"]
            P2["Phase 2\nIdentity Verify"]
            P3["Phase 3\nMFA Confirm"]
            P1 --> P2 --> P3
        end

        subgraph Handlers["Operation Handlers (@handle_cloud_exceptions)"]
            H1["_op_delete_node\n⚠ CRITICAL"]
            H2["_op_restart_node\n🔶 HIGH"]
            H3["_op_batch_update_devices\n🔷 MEDIUM"]
            H4["_op_health_check\n✅ LOW"]
            H5["_op_generic\n(fallback)"]
        end
    end

    subgraph Exceptions["Exception Hierarchy"]
        CE2["CloudOpsError"]
        TE["TransientCloudError\n(retryable)"]
        PE["PermanentCloudError"]
        SE["SecurityViolationError"]
        IE["IdentityVerificationError"]
        TCE["TaskCancelledError"]
        CE2 --> TE
        CE2 --> PE --> SE --> IE
        CE2 --> TCE
    end

    subgraph Observability["Observability"]
        TLOG["_trace_logger\n(UUIDv4 per task)"]
        ELK["ELK / Loki\n(structured grep)"]
        TLOG --> ELK
    end

    CLI -->|TaskContext| CE --> EM
    RAG -.->|rag_hint| CE
    AC -->|injected| EM
    AC -->|injected| Gateway
    EM --> SEM --> Gateway
    Gateway --> Handlers
    EM --> TOKEN
    TOKEN -.->|raise_if_cancelled| Handlers
    Handlers --> TLOG
    EM --> TLOG

    style H1 fill:#FADBD8,stroke:#922B21
    style H2 fill:#FDEBD0,stroke:#CA6F1E
    style H3 fill:#D6EAF8,stroke:#1A5276
    style H4 fill:#D5F5E3,stroke:#1E8449
    style Gateway fill:#F9EBEA,stroke:#922B21
    style Config fill:#FDFEFE,stroke:#AAB7B8
```

---

## Quick Start

```bash
# Clone and navigate
git clone https://github.com/your-handle/cloud-ops-ai-agent.git
cd cloud-ops-ai-agent

# Install dependencies (Python 3.11+ required)
pip install -r requirements.txt

# Run the built-in smoke test (validates full pipeline end-to-end)
python execution_manager.py
```

**Example: Programmatic usage**

```python
import asyncio
from execution_manager import create_engine, TaskContext

async def main():
    engine = create_engine()          # reads config.json automatically

    # LOW risk — no verification required
    result = await engine.execute(
        TaskContext(
            operation="health_check",
            operator_id="sre-alice",
            session_token="<bearer-token>",
        )
    )

    # CRITICAL risk — identity + MFA required
    result = await engine.execute(
        TaskContext(
            operation="delete_node",
            payload={"node_id": "node-007"},
            operator_id="admin-bob",
            session_token="<bearer-token>",
            mfa_token="<totp-code>",
        )
    )

asyncio.run(main())
```

---

## Configuration Reference

All tunables live in `config.json`. Key sections:

| Section | Key Fields | Purpose |
|---|---|---|
| `execution` | `batch_concurrency_limit`, `cancellation_check_interval_seconds` | Semaphore size; abort checkpoint frequency |
| `retry` | `max_attempts`, `base_backoff_seconds`, `backoff_multiplier`, `jitter_factor` | Exponential backoff policy |
| `safety_gateway` | `identity_verification_timeout_seconds`, `session_token_ttl_seconds` | IdP call timeout; session cache TTL |
| `risk_levels` | Per-level `operations[]`, `requires_identity_verification`, `requires_mfa` | Operation classification and gate policy |
| `rag_integration` | `enabled`, `knowledge_base_url`, `top_k_results` | Future RAG context injection |

---

## Roadmap

- [ ] **RAG Integration** — Inject `rag_hint` context from a vector knowledge base into `TaskContext` before dispatch, enabling AI-guided operation selection and parameter validation.
- [ ] **Circuit Breaker** — Wrap each operation handler with a per-target circuit breaker to prevent retries from amplifying a downstream outage.
- [ ] **Prometheus Metrics** — Expose `tasks_total`, `task_duration_seconds`, `safety_violations_total` as Prometheus counters/histograms.
- [ ] **OpenTelemetry Spans** — Propagate `trace_id` as an OTLP trace context for distributed tracing across microservice boundaries.
- [ ] **gRPC Transport Adapter** — Replace mock `_idp_verify` and `_op_*` stubs with real gRPC service bindings.

---

## License

MIT © 2026 — Built on patterns from Baidu Cloud-Phone (红手指) scalable operations infrastructure.
