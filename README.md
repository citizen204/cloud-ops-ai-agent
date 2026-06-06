# cloud-ops-ai-agent

An AI-driven cloud operations management framework with built-in safety gates,
designed for production-grade automation scenarios.

## Design Philosophy

This project applies two patterns refined during large-scale mobile-cloud platform work:

| Pattern | Origin | Implementation |
|---|---|---|
| **Task Termination** | Automated batch optimisation — long-running jobs need a clean abort path | `ExecutionManager.terminate_task()` sets a cooperative cancel flag checked at every safe checkpoint |
| **Confirmation Workflow (二次确认)** | High-impact automated operations require human sign-off before side effects | `ExecutionManager.request_confirmation()` blocks with a configurable timeout |
| **Config-driven Risk Classification (配置化)** | Feature behaviour controlled by external config, not hardcoded logic | `config.json → high_risk_operations` list; promote/demote ops without code changes |

## Project Structure

```
cloud-ops-ai-agent/
├── config.json                        # Operational configuration (high-risk ops, timeouts)
├── requirements.txt
├── cloud_ops_ai_agent/
│   ├── __init__.py
│   ├── execution_manager.py           # Core ExecutionManager class
│   └── exceptions.py                  # Custom exception hierarchy
└── tests/
    └── test_execution_manager.py
```

## Quick Start

```bash
pip install -r requirements.txt
```

```python
from cloud_ops_ai_agent import ExecutionManager

manager = ExecutionManager(config_path="config.json")

# Safe operation — runs immediately
manager.execute_task(
    operation="list_instances",
    resource="us-central1",
    action=lambda: print("listing…"),
)

# High-risk operation — pauses for confirmation
manager.execute_task(
    operation="delete_instance",
    resource="prod-vm-42",
    action=lambda: cloud_client.delete("prod-vm-42"),
)
# → [HIGH RISK] Operation 'delete_instance' on resource 'prod-vm-42'
#   requires confirmation. Type 'CONFIRM' to proceed:
```

## Running Tests

```bash
pytest tests/ -v --cov=cloud_ops_ai_agent
```

## Configuration Reference (`config.json`)

| Key | Description |
|---|---|
| `high_risk_operations` | List of operation names that trigger the confirmation workflow |
| `confirmation.timeout_seconds` | Seconds to wait for operator input before aborting |
| `confirmation.confirm_token` | Exact string the operator must type to proceed |
| `execution.max_retries` | Retry attempts on transient failures |
| `execution.retry_delay_seconds` | Base delay between retries (doubles each attempt) |
