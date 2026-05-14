"""Throttler demo worker.

Registers ``PermitSlotWorkflow`` (the throttler primitive) plus the three
demo workflows and their activities on a single task queue. In production
you would typically run the throttler primitive on its own queue so that
backpressure on demo workers can't block permit acquisition; for the demo
a single queue keeps the surface area small.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from dotenv import load_dotenv
from temporalio.worker import Worker

load_dotenv()

from examples.activities import ALL_ACTIVITIES  # noqa: E402
from examples.gate_workflow import ThrottledGateWorkflow  # noqa: E402
from examples.gpu_workflow import GpuTrainingWorkflow  # noqa: E402
from examples.ingest_workflow import IngestDocumentWorkflow  # noqa: E402
from throttler.config import (  # noqa: E402
    TEMPORAL_ADDRESS,
    TEMPORAL_API_KEY,
    TEMPORAL_NAMESPACE,
    TEMPORAL_TASK_QUEUE,
    connect_temporal_client,
)
from throttler.workflow import PermitSlotWorkflow  # noqa: E402


async def _run_worker() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
    )
    logger = logging.getLogger("throttler.worker")

    client = await connect_temporal_client()

    worker = Worker(
        client,
        task_queue=TEMPORAL_TASK_QUEUE,
        workflows=[
            PermitSlotWorkflow,
            GpuTrainingWorkflow,
            IngestDocumentWorkflow,
            ThrottledGateWorkflow,
        ],
        activities=ALL_ACTIVITIES,
    )

    logger.info(
        "worker started: address=%s namespace=%s task_queue=%s auth=%s",
        TEMPORAL_ADDRESS,
        TEMPORAL_NAMESPACE,
        TEMPORAL_TASK_QUEUE,
        "api-key" if TEMPORAL_API_KEY else "none",
    )

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    async with worker:
        await stop_event.wait()
        logger.info("shutting down worker")


def main() -> None:
    asyncio.run(_run_worker())


if __name__ == "__main__":
    main()
