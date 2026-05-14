"""CLI starter -- launch a burst of demo workflows from the command line.

Usage:
    uv run python starter.py gpu --count 10
    uv run python starter.py ingest --count 12
    uv run python starter.py gate --count 20
"""

from __future__ import annotations

import argparse
import asyncio
import uuid

from dotenv import load_dotenv

load_dotenv()

from examples.gate_workflow import GateInput, ThrottledGateWorkflow  # noqa: E402
from examples.gpu_workflow import GpuTrainingInput, GpuTrainingWorkflow  # noqa: E402
from examples.ingest_workflow import (  # noqa: E402
    IngestDocumentInput,
    IngestDocumentWorkflow,
)
from throttler.config import (  # noqa: E402
    TEMPORAL_TASK_QUEUE,
    connect_temporal_client,
)

SCENARIOS = ("gpu", "ingest", "gate")


async def _start_burst(scenario: str, count: int) -> list[str]:
    client = await connect_temporal_client()
    run_id = uuid.uuid4().hex[:6]
    started: list[str] = []
    for i in range(count):
        wf_id = f"{scenario}-{run_id}-{i}"
        if scenario == "gpu":
            await client.start_workflow(
                GpuTrainingWorkflow.run,
                GpuTrainingInput(model_id=f"model-{run_id}-{i}"),
                id=wf_id,
                task_queue=TEMPORAL_TASK_QUEUE,
            )
        elif scenario == "ingest":
            await client.start_workflow(
                IngestDocumentWorkflow.run,
                IngestDocumentInput(doc_id=f"doc-{run_id}-{i}"),
                id=wf_id,
                task_queue=TEMPORAL_TASK_QUEUE,
            )
        elif scenario == "gate":
            await client.start_workflow(
                ThrottledGateWorkflow.run,
                GateInput(job_id=f"job-{run_id}-{i}"),
                id=wf_id,
                task_queue=TEMPORAL_TASK_QUEUE,
            )
        else:
            raise ValueError(f"unknown scenario: {scenario}")
        started.append(wf_id)
    return started


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("scenario", choices=SCENARIOS)
    parser.add_argument("--count", "-n", type=int, default=10)
    args = parser.parse_args()

    started = asyncio.run(_start_burst(args.scenario, args.count))
    print(f"started {len(started)} {args.scenario} workflows")
    for wf in started:
        print(f"  - {wf}")


if __name__ == "__main__":
    main()
