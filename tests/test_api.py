"""Integration tests for the cloud-ops-ai-agent REST API.

Uses FastAPI's synchronous TestClient (starlette.testclient) so no
pytest-asyncio or event-loop configuration is required.

Covers all seven endpoints:
  GET  /health
  GET  /api/v1/operations
  POST /api/v1/tasks
  GET  /api/v1/tasks
  GET  /api/v1/tasks/{task_id}
  POST /api/v1/tasks/{task_id}/confirm
  DELETE /api/v1/tasks/{task_id}
"""

from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from cloud_ops_ai_agent.api.main import create_app


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def client():
    """Module-scoped TestClient so the lifespan runs once per test module."""
    app = create_app()
    with TestClient(app) as c:
        yield c


def _poll(client: TestClient, task_id: str, target_statuses: set[str],
          timeout: float = 5.0) -> dict:
    """Poll GET /api/v1/tasks/{task_id} until status is in *target_statuses*."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = client.get(f"/api/v1/tasks/{task_id}")
        assert r.status_code == 200
        data = r.json()
        if data["status"] in target_statuses:
            return data
        time.sleep(0.05)
    pytest.fail(
        f"Task {task_id!r} did not reach {target_statuses} within {timeout}s. "
        f"Last status: {r.json()['status']!r}"
    )


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------


class TestHealth:
    def test_status_ok(self, client: TestClient) -> None:
        r = client.get("/health")
        assert r.status_code == 200
        body = r.json()
        assert body["status"] == "ok"
        assert "version" in body

    def test_high_risk_operations_listed(self, client: TestClient) -> None:
        r = client.get("/health")
        ops = r.json()["high_risk_operations"]
        assert isinstance(ops, list)
        assert "delete_instance" in ops


# ---------------------------------------------------------------------------
# GET /api/v1/operations
# ---------------------------------------------------------------------------


class TestListOperations:
    def test_returns_list(self, client: TestClient) -> None:
        r = client.get("/api/v1/operations")
        assert r.status_code == 200
        ops = r.json()
        assert isinstance(ops, list)
        assert len(ops) > 0

    def test_operation_schema(self, client: TestClient) -> None:
        r = client.get("/api/v1/operations")
        op = next(o for o in r.json() if o["name"] == "list_instances")
        assert op["risk"] == "safe"
        assert op["params"] == []
        assert "description" in op

    def test_high_risk_marked(self, client: TestClient) -> None:
        r = client.get("/api/v1/operations")
        op = next(o for o in r.json() if o["name"] == "delete_instance")
        assert op["risk"] == "high"

    def test_all_required_fields_present(self, client: TestClient) -> None:
        r = client.get("/api/v1/operations")
        for op in r.json():
            assert {"name", "description", "risk", "params"} <= op.keys()


# ---------------------------------------------------------------------------
# POST /api/v1/tasks — submission
# ---------------------------------------------------------------------------


class TestSubmitTask:
    def test_safe_task_accepted(self, client: TestClient) -> None:
        r = client.post(
            "/api/v1/tasks",
            json={"operation": "list_instances", "resource": "all", "params": {}},
        )
        assert r.status_code == 202
        body = r.json()
        assert "task_id" in body
        assert body["is_high_risk"] is False
        assert body["status"] == "PENDING"

    def test_high_risk_task_accepted(self, client: TestClient) -> None:
        r = client.post(
            "/api/v1/tasks",
            json={
                "operation": "delete_instance",
                "resource": "vm-dev-01",
                "params": {"instance_id": "vm-dev-01"},
            },
        )
        assert r.status_code == 202
        body = r.json()
        assert body["is_high_risk"] is True
        # Confirm immediately so the task doesn't block other tests
        task_id = body["task_id"]
        _poll(client, task_id, {"AWAITING_CONFIRMATION"})
        client.post(
            f"/api/v1/tasks/{task_id}/confirm",
            json={"confirmed": False},
        )

    def test_unknown_operation_returns_400(self, client: TestClient) -> None:
        r = client.post(
            "/api/v1/tasks",
            json={"operation": "launch_rockets", "resource": "moon", "params": {}},
        )
        assert r.status_code == 400
        assert "launch_rockets" in r.json()["detail"]

    def test_missing_required_params_returns_422(self, client: TestClient) -> None:
        r = client.post(
            "/api/v1/tasks",
            json={
                "operation": "describe_instance",
                "resource": "vm-prod-01",
                "params": {},          # instance_id is required
            },
        )
        assert r.status_code == 422
        assert "instance_id" in r.json()["detail"]

    def test_message_mentions_confirm_for_high_risk(self, client: TestClient) -> None:
        r = client.post(
            "/api/v1/tasks",
            json={
                "operation": "delete_instance",
                "resource": "vm-staging-01",
                "params": {"instance_id": "vm-staging-01"},
            },
        )
        body = r.json()
        assert "confirm" in body["message"].lower()
        # Clean up
        task_id = body["task_id"]
        _poll(client, task_id, {"AWAITING_CONFIRMATION"})
        client.post(f"/api/v1/tasks/{task_id}/confirm", json={"confirmed": False})


# ---------------------------------------------------------------------------
# GET /api/v1/tasks
# ---------------------------------------------------------------------------


class TestListTasks:
    def test_returns_list(self, client: TestClient) -> None:
        r = client.get("/api/v1/tasks")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    def test_submitted_task_appears_in_list(self, client: TestClient) -> None:
        submit = client.post(
            "/api/v1/tasks",
            json={"operation": "list_instances", "resource": "all", "params": {}},
        )
        task_id = submit.json()["task_id"]
        r = client.get("/api/v1/tasks")
        ids = [t["task_id"] for t in r.json()]
        assert task_id in ids


# ---------------------------------------------------------------------------
# GET /api/v1/tasks/{task_id}
# ---------------------------------------------------------------------------


class TestGetTask:
    def test_known_task_returned(self, client: TestClient) -> None:
        submit = client.post(
            "/api/v1/tasks",
            json={"operation": "list_instances", "resource": "all", "params": {}},
        )
        task_id = submit.json()["task_id"]
        r = client.get(f"/api/v1/tasks/{task_id}")
        assert r.status_code == 200
        assert r.json()["task_id"] == task_id

    def test_unknown_task_returns_404(self, client: TestClient) -> None:
        r = client.get("/api/v1/tasks/nonexistent-task-id-xyz")
        assert r.status_code == 404

    def test_safe_task_completes(self, client: TestClient) -> None:
        submit = client.post(
            "/api/v1/tasks",
            json={"operation": "describe_instance", "resource": "vm-prod-01",
                  "params": {"instance_id": "vm-prod-01"}},
        )
        task_id = submit.json()["task_id"]
        data = _poll(client, task_id, {"COMPLETED", "FAILED"})
        assert data["status"] == "COMPLETED"
        assert data["result"] is not None

    def test_task_response_schema(self, client: TestClient) -> None:
        submit = client.post(
            "/api/v1/tasks",
            json={"operation": "list_instances", "resource": "all", "params": {}},
        )
        task_id = submit.json()["task_id"]
        data = _poll(client, task_id, {"COMPLETED", "FAILED"})
        assert {"task_id", "operation", "resource", "status",
                "result", "error", "created_at", "updated_at"} <= data.keys()


# ---------------------------------------------------------------------------
# POST /api/v1/tasks/{task_id}/confirm
# ---------------------------------------------------------------------------


class TestConfirmTask:
    def _submit_high_risk(self, client: TestClient,
                          resource: str = "vm-prod-02") -> str:
        r = client.post(
            "/api/v1/tasks",
            json={
                "operation": "delete_instance",
                "resource": resource,
                "params": {"instance_id": resource},
            },
        )
        assert r.status_code == 202
        return r.json()["task_id"]

    def test_confirm_approve_leads_to_completed(self, client: TestClient) -> None:
        task_id = self._submit_high_risk(client, "vm-prod-01")
        _poll(client, task_id, {"AWAITING_CONFIRMATION"})

        r = client.post(
            f"/api/v1/tasks/{task_id}/confirm",
            json={"confirmed": True},
        )
        assert r.status_code == 200
        assert r.json()["confirmed"] is True

        data = _poll(client, task_id, {"COMPLETED", "FAILED", "TERMINATED"}, timeout=10.0)
        assert data["status"] == "COMPLETED"

    def test_confirm_reject_leads_to_failed(self, client: TestClient) -> None:
        task_id = self._submit_high_risk(client, "vm-staging-01")
        _poll(client, task_id, {"AWAITING_CONFIRMATION"})

        r = client.post(
            f"/api/v1/tasks/{task_id}/confirm",
            json={"confirmed": False},
        )
        assert r.status_code == 200
        assert r.json()["confirmed"] is False

        data = _poll(client, task_id, {"FAILED", "TERMINATED"}, timeout=5.0)
        assert data["status"] == "FAILED"

    def test_confirm_non_pending_task_returns_409(self, client: TestClient) -> None:
        # A safe completed task has no confirmation event
        submit = client.post(
            "/api/v1/tasks",
            json={"operation": "list_instances", "resource": "all", "params": {}},
        )
        task_id = submit.json()["task_id"]
        _poll(client, task_id, {"COMPLETED", "FAILED"})

        r = client.post(
            f"/api/v1/tasks/{task_id}/confirm",
            json={"confirmed": True},
        )
        assert r.status_code == 409

    def test_confirm_unknown_task_returns_409(self, client: TestClient) -> None:
        r = client.post(
            "/api/v1/tasks/no-such-task/confirm",
            json={"confirmed": True},
        )
        assert r.status_code == 409


# ---------------------------------------------------------------------------
# DELETE /api/v1/tasks/{task_id}
# ---------------------------------------------------------------------------


class TestTerminateTask:
    def test_terminate_running_task(self, client: TestClient) -> None:
        # restart_instance takes 1.5s — enough time to send the terminate signal
        submit = client.post(
            "/api/v1/tasks",
            json={
                "operation": "restart_instance",
                "resource": "vm-prod-01",
                "params": {"instance_id": "vm-prod-01"},
            },
        )
        task_id = submit.json()["task_id"]
        _poll(client, task_id, {"RUNNING", "PENDING"})

        r = client.delete(f"/api/v1/tasks/{task_id}")
        assert r.status_code == 200
        assert r.json()["signalled"] is True

    def test_terminate_unknown_task_returns_409(self, client: TestClient) -> None:
        r = client.delete("/api/v1/tasks/ghost-task-id")
        assert r.status_code == 409

    def test_terminate_already_completed_task_returns_409(
        self, client: TestClient
    ) -> None:
        submit = client.post(
            "/api/v1/tasks",
            json={"operation": "list_instances", "resource": "all", "params": {}},
        )
        task_id = submit.json()["task_id"]
        _poll(client, task_id, {"COMPLETED", "FAILED"})

        r = client.delete(f"/api/v1/tasks/{task_id}")
        assert r.status_code == 409
