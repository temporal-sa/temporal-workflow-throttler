"""Shared pytest helpers for throttler tests.

Each test runs against a fresh in-process ``WorkflowEnvironment`` started with
time-skipping enabled. Time-skipping is critical for these tests:

- ``workflow.sleep(...)`` (used by the throttler's backoff loop and by lease
  timers) is fast-forwarded automatically when no workflow has progress to
  make, so retries and lease expiry are observed in milliseconds.
- ``env.sleep(...)`` from test code advances workflow time without a real
  wall-clock wait.

Tests follow the standard Temporal mocking pattern: register mock activities
with the same ``name=`` as the production ones, and assert on captured calls
and workflow results. No real I/O, no real sleeps -- safe for CI/CD.

The ``with_env`` context manager retries server startup briefly to absorb the
small port-reuse race between consecutive tests, and proactively terminates
any abandoned ``PermitSlotWorkflow`` executions before shutting the server
down so the test process exits cleanly.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import AsyncIterator

from temporalio.testing import WorkflowEnvironment

from throttler.config import PERMIT_WORKFLOW_ID_PREFIX

_START_RETRIES = 5
_START_TIMEOUT = 10.0
_START_RETRY_BACKOFF = 0.5


async def _start_env() -> WorkflowEnvironment:
    last_err: Exception | None = None
    for attempt in range(_START_RETRIES):
        try:
            return await asyncio.wait_for(
                WorkflowEnvironment.start_time_skipping(),
                timeout=_START_TIMEOUT,
            )
        except (Exception, asyncio.TimeoutError) as e:
            last_err = e
            await asyncio.sleep(_START_RETRY_BACKOFF * (attempt + 1))
    assert last_err is not None
    raise last_err


async def _terminate_lingering_slots(env: WorkflowEnvironment) -> None:
    """ABANDON child workflows can outlive their parents -- terminate any that
    are still RUNNING so the test server has nothing in flight at shutdown."""
    try:
        async for w in env.client.list_workflows(
            f'WorkflowId STARTS_WITH "{PERMIT_WORKFLOW_ID_PREFIX}:" '
            f'AND ExecutionStatus = "Running"'
        ):
            try:
                handle = env.client.get_workflow_handle(w.id, run_id=w.run_id)
                await handle.terminate(reason="test cleanup")
            except Exception:
                pass
    except Exception:
        pass


@asynccontextmanager
async def with_env() -> AsyncIterator[WorkflowEnvironment]:
    env = await _start_env()
    try:
        yield env
    finally:
        await _terminate_lingering_slots(env)
        await env.shutdown()
