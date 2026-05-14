"""Shared constants and connection helpers for the throttler library and
demo workers."""

from __future__ import annotations

import os
from datetime import timedelta

from temporalio.client import Client

TEMPORAL_ADDRESS: str = os.environ.get("TEMPORAL_ADDRESS", "localhost:7233")
TEMPORAL_NAMESPACE: str = os.environ.get("TEMPORAL_NAMESPACE", "default")
TEMPORAL_TASK_QUEUE: str = os.environ.get("TEMPORAL_TASK_QUEUE", "throttler-tq")
TEMPORAL_API_KEY: str | None = os.environ.get("TEMPORAL_API_KEY") or None
TEMPORAL_TLS: bool = os.environ.get("TEMPORAL_TLS", "").lower() in {"1", "true", "yes"}

DEFAULT_LEASE: timedelta = timedelta(minutes=10)
DEFAULT_BACKOFF: timedelta = timedelta(seconds=5)

PERMIT_WORKFLOW_ID_PREFIX: str = "permit"


def permit_workflow_id(resource: str, slot: str) -> str:
    """Workflow ID for a single held permit. The existence of a running workflow
    with this ID is the lock; Temporal enforces ID uniqueness atomically."""
    return f"{PERMIT_WORKFLOW_ID_PREFIX}:{resource}:{slot}"


async def connect_temporal_client() -> Client:
    """Connect to Temporal using the configured environment.

    Behaves three ways:

    1. ``TEMPORAL_API_KEY`` set -> Temporal Cloud-style API-key auth with TLS
       always on (the API key implies TLS).
    2. ``TEMPORAL_TLS=true`` (no API key) -> system-trust TLS for self-hosted
       deployments fronted by TLS without API-key auth.
    3. Neither set -> plain TCP, suitable for ``temporal server start-dev``.
    """
    if TEMPORAL_API_KEY:
        return await Client.connect(
            TEMPORAL_ADDRESS,
            namespace=TEMPORAL_NAMESPACE,
            api_key=TEMPORAL_API_KEY,
            tls=True,
        )
    if TEMPORAL_TLS:
        return await Client.connect(
            TEMPORAL_ADDRESS,
            namespace=TEMPORAL_NAMESPACE,
            tls=True,
        )
    return await Client.connect(
        TEMPORAL_ADDRESS,
        namespace=TEMPORAL_NAMESPACE,
    )
