"""Simulated cloud operations for demonstration and development use.

Each function in this module represents a realistic cloud-platform action.
They introduce artificial latency to mimic real API round-trips and return
structured payloads that a frontend or CLI can render meaningfully.

In a production integration, replace these stubs with calls to cloud provider
SDKs (e.g. ``google-cloud-compute``, ``boto3``, ``azure-mgmt-compute``).
"""

from __future__ import annotations

import random
import time
from typing import Any

# Simulated cloud resource inventory
_INSTANCES: dict[str, dict[str, Any]] = {
    "vm-prod-01": {"zone": "us-central1-a", "status": "RUNNING", "type": "n2-standard-4"},
    "vm-prod-02": {"zone": "us-central1-b", "status": "RUNNING", "type": "n2-standard-4"},
    "vm-staging-01": {"zone": "us-central1-a", "status": "RUNNING", "type": "e2-medium"},
    "vm-dev-01": {"zone": "us-east1-b", "status": "STOPPED", "type": "e2-micro"},
}

_CLUSTERS: dict[str, dict[str, Any]] = {
    "cluster-prod": {"nodes": 8, "region": "us-central1", "status": "HEALTHY"},
    "cluster-staging": {"nodes": 3, "region": "us-central1", "status": "HEALTHY"},
}


def list_instances() -> dict[str, Any]:
    """Return a summary of all simulated VM instances.

    Returns:
        A dict with ``instances`` (list of instance summaries) and
        ``total_count``.
    """
    time.sleep(0.3)
    instances = [
        {"id": iid, **attrs} for iid, attrs in _INSTANCES.items()
    ]
    return {"instances": instances, "total_count": len(instances)}


def describe_instance(instance_id: str) -> dict[str, Any]:
    """Return detailed metadata for a single instance.

    Args:
        instance_id: The identifier of the instance to describe.

    Returns:
        A dict containing the instance metadata.

    Raises:
        KeyError: If ``instance_id`` is not found.
    """
    time.sleep(0.2)
    if instance_id not in _INSTANCES:
        raise KeyError(f"Instance '{instance_id}' not found.")
    return {"id": instance_id, **_INSTANCES[instance_id]}


def restart_instance(instance_id: str) -> dict[str, Any]:
    """Simulate a graceful restart of a running instance.

    Args:
        instance_id: The identifier of the instance to restart.

    Returns:
        A dict with ``status`` and ``message``.
    """
    time.sleep(1.5)
    if instance_id not in _INSTANCES:
        raise KeyError(f"Instance '{instance_id}' not found.")
    return {
        "status": "RESTARTED",
        "instance_id": instance_id,
        "message": f"Instance '{instance_id}' successfully restarted.",
    }


def delete_instance(instance_id: str) -> dict[str, Any]:
    """Permanently delete a VM instance (HIGH RISK).

    Args:
        instance_id: The identifier of the instance to delete.

    Returns:
        A dict confirming deletion.
    """
    time.sleep(2.0)
    _INSTANCES.pop(instance_id, None)
    return {
        "status": "DELETED",
        "instance_id": instance_id,
        "message": f"Instance '{instance_id}' has been permanently deleted.",
    }


def scale_down_cluster(cluster_id: str, target_nodes: int = 1) -> dict[str, Any]:
    """Reduce the node count of a cluster (HIGH RISK).

    Args:
        cluster_id: Identifier of the target cluster.
        target_nodes: Desired node count after scale-down.

    Returns:
        A dict with the new cluster state.
    """
    time.sleep(3.0)
    if cluster_id not in _CLUSTERS:
        raise KeyError(f"Cluster '{cluster_id}' not found.")
    previous = _CLUSTERS[cluster_id]["nodes"]
    _CLUSTERS[cluster_id]["nodes"] = target_nodes
    return {
        "status": "SCALED_DOWN",
        "cluster_id": cluster_id,
        "previous_nodes": previous,
        "current_nodes": target_nodes,
        "message": f"Cluster '{cluster_id}' scaled from {previous} → {target_nodes} nodes.",
    }


def deploy_to_production(service_name: str, image_tag: str) -> dict[str, Any]:
    """Roll out a new container image to the production environment (HIGH RISK).

    Args:
        service_name: The name of the service to update.
        image_tag: The image tag to deploy (e.g. ``"v2.3.1"``).

    Returns:
        A dict confirming the deployment.
    """
    time.sleep(4.0)
    build_id = f"build-{random.randint(10000, 99999)}"
    return {
        "status": "DEPLOYED",
        "service_name": service_name,
        "image_tag": image_tag,
        "build_id": build_id,
        "message": (
            f"Service '{service_name}' successfully updated to image '{image_tag}' "
            f"(build: {build_id})."
        ),
    }


# Registry: operation_name -> (callable, description, required_params)
OPERATION_REGISTRY: dict[str, dict[str, Any]] = {
    "list_instances": {
        "fn": list_instances,
        "description": "List all VM instances",
        "risk": "safe",
        "params": [],
    },
    "describe_instance": {
        "fn": describe_instance,
        "description": "Get details for a single VM instance",
        "risk": "safe",
        "params": ["instance_id"],
    },
    "restart_instance": {
        "fn": restart_instance,
        "description": "Gracefully restart a running VM instance",
        "risk": "safe",
        "params": ["instance_id"],
    },
    "delete_instance": {
        "fn": delete_instance,
        "description": "Permanently delete a VM instance",
        "risk": "high",
        "params": ["instance_id"],
    },
    "scale_down_cluster": {
        "fn": scale_down_cluster,
        "description": "Reduce the node count of a Kubernetes cluster",
        "risk": "high",
        "params": ["cluster_id", "target_nodes"],
    },
    "deploy_to_production": {
        "fn": deploy_to_production,
        "description": "Deploy a new container image to the production environment",
        "risk": "high",
        "params": ["service_name", "image_tag"],
    },
}
