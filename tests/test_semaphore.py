"""Semaphore contention tests using a signal-driven harness.

The harness workflow ``HoldSlotWorkflow`` acquires a slot, then blocks on a
``proceed`` signal before releasing. This lets the test deterministically
sequence "acquired" / "release" without any real-time sleeps -- time-skipping
fast-forwards over the throttler's internal backoff timers.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.client import WorkflowExecutionStatus
from temporalio.worker import Worker

from tests.conftest import with_env
from throttler.config import permit_workflow_id
from throttler.workflow import PermitSlotWorkflow, Semaphore

TASK_QUEUE = "throttler-tq-semaphore-test"


@workflow.defn(name="HoldSlotWorkflow")
class HoldSlotWorkflow:
    """Acquire a slot, wait for ``proceed`` signal, then release."""

    def __init__(self) -> None:
        self._proceed = False

    @workflow.signal(name="proceed")
    def proceed(self) -> None:
        self._proceed = True

    @workflow.run
    async def run(self, resource: str, capacity: int) -> str:
        sem = Semaphore(resource, capacity=capacity)
        async with sem.acquire(
            lease=timedelta(minutes=5),
            backoff=timedelta(seconds=1),
        ) as slot:
            await workflow.wait_condition(lambda: self._proceed)
            return slot


async def test_capacity_one_serializes_two_callers() -> None:
    async with with_env() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[HoldSlotWorkflow, PermitSlotWorkflow],
        ):
            first = await env.client.start_workflow(
                HoldSlotWorkflow.run,
                args=["cap1", 1],
                id="hold-cap1-1",
                task_queue=TASK_QUEUE,
            )
            second = await env.client.start_workflow(
                HoldSlotWorkflow.run,
                args=["cap1", 1],
                id="hold-cap1-2",
                task_queue=TASK_QUEUE,
            )

            # let both workflows progress through their first acquire attempt
            await env.sleep(timedelta(seconds=10))

            # first holds the slot, second is queued in its backoff loop
            assert (await first.describe()).status == WorkflowExecutionStatus.RUNNING
            assert (await second.describe()).status == WorkflowExecutionStatus.RUNNING

            slot = env.client.get_workflow_handle(permit_workflow_id("cap1", "0"))
            assert await slot.query("held") is True

            # release first -> second can take the slot
            await first.signal("proceed")
            assert await first.result() == "0"

            await second.signal("proceed")
            assert await second.result() == "0"


async def _count_held(
    env, resource: str, capacity: int
) -> int:
    held = 0
    for slot_idx in range(capacity):
        handle = env.client.get_workflow_handle(
            permit_workflow_id(resource, str(slot_idx))
        )
        try:
            desc = await handle.describe()
            if desc.status == WorkflowExecutionStatus.RUNNING:
                held += 1
        except Exception:
            pass
    return held


async def test_capacity_three_caps_concurrent_holders() -> None:
    """6 callers compete for a 3-slot pool; at any moment, at most 3 hold."""
    async with with_env() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[HoldSlotWorkflow, PermitSlotWorkflow],
        ):
            capacity = 3
            burst = 6
            handles = [
                await env.client.start_workflow(
                    HoldSlotWorkflow.run,
                    args=["cap3", capacity],
                    id=f"hold-cap3-{i}",
                    task_queue=TASK_QUEUE,
                )
                for i in range(burst)
            ]

            # poll for steady state: all `capacity` slots held, with bounded
            # simulated time so a real bug shows up as a failure rather than
            # a hang. With time-skipping, env.sleep is essentially instant.
            held = 0
            for _ in range(30):
                await env.sleep(timedelta(seconds=2))
                held = await _count_held(env, "cap3", capacity)
                if held == capacity:
                    break
            assert held == capacity, (
                f"expected exactly {capacity} slots held, observed {held}"
            )

            # release all: each waiter gets its turn
            for h in handles:
                await h.signal("proceed")
            results = await asyncio.gather(*[h.result() for h in handles])
            assert len(results) == burst
            assert set(results).issubset({"0", "1", "2"})
