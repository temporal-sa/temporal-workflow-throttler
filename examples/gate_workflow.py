"""Generic workflow-throttling scenario.

Caps the number of concurrent ``ThrottledGateWorkflow`` runs that can be
inside the protected critical section at the same time. Mirrors the original
in-memory ``SemaphoreGateExampleWorkflow`` pattern, but durable across worker
restarts and shared across the whole fleet.

Capacity is passed in as a workflow argument so the UI can change it.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow

from throttler.workflow import Semaphore

GATE_RESOURCE = "app-gate"
DEFAULT_GATE_CAPACITY = 4
WORK_DURATION_SECONDS = 10.0


@dataclass
class GateInput:
    job_id: str
    capacity: int = DEFAULT_GATE_CAPACITY


@workflow.defn(name="ThrottledGateWorkflow")
class ThrottledGateWorkflow:
    @workflow.run
    async def run(self, input: GateInput) -> str:
        sem = Semaphore(GATE_RESOURCE, capacity=input.capacity)
        async with sem.acquire(lease=timedelta(minutes=10)) as slot:
            workflow.logger.info(
                "gate acquired", extra={"job_id": input.job_id, "slot": slot}
            )
            return await workflow.execute_activity(
                "simulate_work",
                args=[WORK_DURATION_SECONDS, f"gate-job-{input.job_id}"],
                start_to_close_timeout=timedelta(minutes=2),
            )
