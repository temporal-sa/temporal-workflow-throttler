"""GPU resource-lock scenario.

Demonstrates the *resource lock* use case: a small fixed pool of named
resources (GPUs), each held by exactly one workflow at a time. The slot name
the caller receives identifies the specific resource, so the activity can
target that physical GPU.

Capacity is taken from the workflow input -- the API/UI controls how many
GPU slots exist by passing ``capacity`` when starting the workflow. Slot
names follow the pattern ``gpu-0``, ``gpu-1``, etc.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow

from throttler.semaphore import Semaphore

GPU_RESOURCE = "gpu-pool"
DEFAULT_GPU_CAPACITY = 4


@dataclass
class GpuTrainingInput:
    model_id: str
    capacity: int = DEFAULT_GPU_CAPACITY


def gpu_slot_names(capacity: int) -> list[str]:
    return [f"gpu-{i}" for i in range(capacity)]


@workflow.defn(name="GpuTrainingWorkflow")
class GpuTrainingWorkflow:
    @workflow.run
    async def run(self, input: GpuTrainingInput) -> str:
        sem = Semaphore(GPU_RESOURCE, slots=gpu_slot_names(input.capacity))
        async with sem.acquire(lease=timedelta(minutes=10)) as gpu_slot:
            workflow.logger.info(
                "got gpu",
                extra={"model_id": input.model_id, "gpu_slot": gpu_slot},
            )
            return await workflow.execute_activity(
                "run_training",
                args=[input.model_id, gpu_slot],
                start_to_close_timeout=timedelta(minutes=5),
            )
