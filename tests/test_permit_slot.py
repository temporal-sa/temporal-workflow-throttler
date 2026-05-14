"""PermitSlotWorkflow lifecycle tests.

These verify the slot workflow's contract:
  * ``release`` signal -> Completes (returns "released")
  * lease timeout    -> Completes (returns "lease_expired")

Both paths use only signals and time-skipping, no real wall-clock waits.
"""

from __future__ import annotations

from datetime import timedelta

from temporalio.client import WorkflowExecutionStatus
from temporalio.worker import Worker

from tests.conftest import with_env
from throttler.config import permit_workflow_id
from throttler.workflow import PermitSlotInput, PermitSlotWorkflow

TASK_QUEUE = "throttler-tq-permit-slot-test"


async def test_release_signal_completes_slot() -> None:
    async with with_env() as env:
        async with Worker(
            env.client, task_queue=TASK_QUEUE, workflows=[PermitSlotWorkflow]
        ):
            handle = await env.client.start_workflow(
                PermitSlotWorkflow.run,
                PermitSlotInput(resource="r", slot="s", lease_seconds=600.0),
                id=permit_workflow_id("r", "s"),
                task_queue=TASK_QUEUE,
            )
            assert await handle.query("held") is True

            await handle.signal("release")
            assert await handle.result() == "released"

            desc = await handle.describe()
            assert desc.status == WorkflowExecutionStatus.COMPLETED


async def test_lease_expires_when_no_release_signal_arrives() -> None:
    async with with_env() as env:
        async with Worker(
            env.client, task_queue=TASK_QUEUE, workflows=[PermitSlotWorkflow]
        ):
            handle = await env.client.start_workflow(
                PermitSlotWorkflow.run,
                PermitSlotInput(resource="r", slot="s2", lease_seconds=60.0),
                id=permit_workflow_id("r", "s2"),
                task_queue=TASK_QUEUE,
            )

            # time-skip past the lease; no real wait
            await env.sleep(timedelta(seconds=120))
            assert await handle.result() == "lease_expired"
