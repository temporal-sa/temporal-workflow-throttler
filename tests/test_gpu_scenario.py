"""Business-logic tests for ``GpuTrainingWorkflow``.

We trust Temporal's workflow-ID uniqueness and the throttler's Semaphore
(both covered elsewhere); the scenario test just verifies the demo workflow
wiring: it acquires a slot from ``gpu-pool`` with the requested capacity and
calls ``run_training`` with the model id and the slot it received.
"""

from __future__ import annotations

from temporalio import activity
from temporalio.worker import Worker

from examples.gpu_workflow import (
    GpuTrainingInput,
    GpuTrainingWorkflow,
    gpu_slot_names,
)
from tests.conftest import with_env
from throttler.workflow import PermitSlotWorkflow

TASK_QUEUE = "throttler-tq-gpu-scenario-test"


async def test_gpu_workflow_calls_training_with_acquired_slot() -> None:
    captured: list[tuple[str, str]] = []

    @activity.defn(name="run_training")
    async def mock_training(model_id: str, gpu_slot: str) -> str:
        captured.append((model_id, gpu_slot))
        return f"trained:{model_id}@{gpu_slot}"

    async with with_env() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[GpuTrainingWorkflow, PermitSlotWorkflow],
            activities=[mock_training],
        ):
            capacity = 4
            valid_slots = set(gpu_slot_names(capacity))
            result = await env.client.execute_workflow(
                GpuTrainingWorkflow.run,
                GpuTrainingInput(model_id="m1", capacity=capacity),
                id="gpu-test-1",
                task_queue=TASK_QUEUE,
            )

            assert len(captured) == 1
            model_id, gpu_slot = captured[0]
            assert model_id == "m1"
            assert gpu_slot in valid_slots
            assert result == f"trained:m1@{gpu_slot}"


async def test_gpu_workflow_respects_custom_capacity() -> None:
    """Using capacity=2 should restrict slot names to gpu-0 / gpu-1."""
    captured: list[str] = []

    @activity.defn(name="run_training")
    async def mock_training(model_id: str, gpu_slot: str) -> str:
        captured.append(gpu_slot)
        return f"ok-{gpu_slot}"

    async with with_env() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[GpuTrainingWorkflow, PermitSlotWorkflow],
            activities=[mock_training],
        ):
            await env.client.execute_workflow(
                GpuTrainingWorkflow.run,
                GpuTrainingInput(model_id="m2", capacity=2),
                id="gpu-test-cap2",
                task_queue=TASK_QUEUE,
            )

            assert captured[0] in {"gpu-0", "gpu-1"}
