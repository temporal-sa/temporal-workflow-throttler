"""Business-logic tests for ``ThrottledGateWorkflow``.

Verifies the workflow acquires an ``app-gate`` slot and calls
``simulate_work`` with the configured duration and a label derived from the
job id. Concurrency invariants are covered by ``test_semaphore.py``.
"""

from __future__ import annotations

from temporalio import activity
from temporalio.worker import Worker

from examples.gate_workflow import (
    GateInput,
    ThrottledGateWorkflow,
    WORK_DURATION_SECONDS,
)
from tests.conftest import with_env
from throttler.workflow import PermitSlotWorkflow

TASK_QUEUE = "throttler-tq-gate-scenario-test"


async def test_gate_workflow_calls_simulate_work_with_expected_args() -> None:
    captured: list[tuple[float, str]] = []

    @activity.defn(name="simulate_work")
    async def mock_simulate(duration_seconds: float, label: str) -> str:
        captured.append((duration_seconds, label))
        return f"done:{label}"

    async with with_env() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[ThrottledGateWorkflow, PermitSlotWorkflow],
            activities=[mock_simulate],
        ):
            result = await env.client.execute_workflow(
                ThrottledGateWorkflow.run,
                GateInput(job_id="j1", capacity=4),
                id="gate-test-1",
                task_queue=TASK_QUEUE,
            )

            assert len(captured) == 1
            duration, label = captured[0]
            assert duration == WORK_DURATION_SECONDS
            assert label == "gate-job-j1"
            assert result == "done:gate-job-j1"
