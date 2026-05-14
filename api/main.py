"""Demo HTTP API for the throttler UI.

- ``GET  /api/scenarios``                  list configured scenarios + current capacity
- ``GET  /api/config``                     full capacity map keyed by resource name
- ``POST /api/config/{resource}?capacity=N`` set a resource's capacity (for new runs)
- ``POST /api/run/{scenario}?count=N``     launch N demo workflows of the given scenario
- ``GET  /api/status/{resource}``          live slot status for a resource
- ``GET  /api/recent/{scenario}?limit=N``  recent workflow runs

Capacity is held in-process on this API and passed as a workflow argument
each time you click Run, so adjusting it in the UI affects future bursts but
not workflows that have already started.
"""

from __future__ import annotations

import logging
import os
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from temporalio.client import Client, WorkflowExecutionStatus

load_dotenv()  # noqa: E402

from examples.gate_workflow import (  # noqa: E402
    DEFAULT_GATE_CAPACITY,
    GATE_RESOURCE,
    GateInput,
    ThrottledGateWorkflow,
)
from examples.gpu_workflow import (  # noqa: E402
    DEFAULT_GPU_CAPACITY,
    GPU_RESOURCE,
    GpuTrainingInput,
    GpuTrainingWorkflow,
    gpu_slot_names,
)
from examples.ingest_workflow import (  # noqa: E402
    DEFAULT_EMBED_CAPACITY,
    DEFAULT_OCR_CAPACITY,
    DEFAULT_STORE_CAPACITY,
    EMBED_RESOURCE,
    IngestDocumentInput,
    IngestDocumentWorkflow,
    OCR_RESOURCE,
    STORE_RESOURCE,
)
from throttler.config import (  # noqa: E402
    TEMPORAL_ADDRESS,
    TEMPORAL_API_KEY,
    TEMPORAL_NAMESPACE,
    TEMPORAL_TASK_QUEUE,
    connect_temporal_client,
    permit_workflow_id,
)

logger = logging.getLogger("throttler.api")

CAPACITY_MIN = 1
CAPACITY_MAX = 16

DEFAULT_CAPACITIES: dict[str, int] = {
    GPU_RESOURCE: DEFAULT_GPU_CAPACITY,
    OCR_RESOURCE: DEFAULT_OCR_CAPACITY,
    EMBED_RESOURCE: DEFAULT_EMBED_CAPACITY,
    STORE_RESOURCE: DEFAULT_STORE_CAPACITY,
    GATE_RESOURCE: DEFAULT_GATE_CAPACITY,
}


def _slot_names(resource: str, capacity: int) -> list[str]:
    if resource == GPU_RESOURCE:
        return gpu_slot_names(capacity)
    return [str(i) for i in range(capacity)]


SCENARIOS: dict[str, dict[str, Any]] = {
    "gpu": {
        "label": "GPU Pool",
        "description": "Resource lock: each workflow gets exclusive access to one named GPU.",
        "id_prefix": "gpu-train",
        "wf_type_name": "GpuTrainingWorkflow",
        "resources": [GPU_RESOURCE],
    },
    "ingest": {
        "label": "Ingest Pipeline",
        "description": (
            "Three nested semaphores along the OCR -> Embed -> Store pipeline. "
            "Each workflow waits its turn at each gate independently."
        ),
        "id_prefix": "ingest",
        "wf_type_name": "IngestDocumentWorkflow",
        "resources": [OCR_RESOURCE, EMBED_RESOURCE, STORE_RESOURCE],
    },
    "gate": {
        "label": "Workflow Gate",
        "description": (
            "Generic concurrency gate: caps how many ThrottledGateWorkflow runs "
            "are inside the protected critical section at the same time."
        ),
        "id_prefix": "gate",
        "wf_type_name": "ThrottledGateWorkflow",
        "resources": [GATE_RESOURCE],
    },
}


@asynccontextmanager
async def _lifespan(app: FastAPI):
    app.state.temporal_client = await connect_temporal_client()
    app.state.capacities = dict(DEFAULT_CAPACITIES)
    logger.info(
        "api connected to temporal at %s/%s (auth=%s)",
        TEMPORAL_ADDRESS,
        TEMPORAL_NAMESPACE,
        "api-key" if TEMPORAL_API_KEY else "none",
    )
    yield


app = FastAPI(title="temporal-workflow-throttler-api", lifespan=_lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("CORS_ORIGINS", "http://localhost:5173").split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _client(request: Request) -> Client:
    return request.app.state.temporal_client


def _capacities(request: Request) -> dict[str, int]:
    return request.app.state.capacities


def _resource_view(name: str, capacity: int) -> dict[str, Any]:
    slots = _slot_names(name, capacity)
    return {"name": name, "slots": slots, "capacity": capacity}


@app.get("/api/scenarios")
async def list_scenarios(request: Request) -> dict[str, Any]:
    caps = _capacities(request)
    return {
        "scenarios": [
            {
                "id": sid,
                "label": s["label"],
                "description": s["description"],
                "resources": [
                    _resource_view(r, caps[r]) for r in s["resources"]
                ],
            }
            for sid, s in SCENARIOS.items()
        ]
    }


@app.get("/api/config")
async def get_config(request: Request) -> dict[str, Any]:
    caps = _capacities(request)
    return {
        "capacities": dict(caps),
        "min": CAPACITY_MIN,
        "max": CAPACITY_MAX,
    }


@app.post("/api/config/{resource}")
async def set_capacity(
    resource: str, request: Request, capacity: int
) -> dict[str, Any]:
    caps = _capacities(request)
    if resource not in caps:
        raise HTTPException(404, f"unknown resource: {resource}")
    if capacity < CAPACITY_MIN or capacity > CAPACITY_MAX:
        raise HTTPException(
            400,
            f"capacity must be between {CAPACITY_MIN} and {CAPACITY_MAX}",
        )
    caps[resource] = capacity
    logger.info("capacity %s = %s", resource, capacity)
    return {"resource": resource, "capacity": capacity}


@app.post("/api/run/{scenario}")
async def run_scenario(
    scenario: str, request: Request, count: int = 1
) -> dict[str, Any]:
    if scenario not in SCENARIOS:
        raise HTTPException(404, f"unknown scenario: {scenario}")
    if count < 1 or count > 100:
        raise HTTPException(400, "count must be between 1 and 100")

    caps = _capacities(request)
    client = _client(request)
    run = uuid.uuid4().hex[:6]
    started: list[str] = []
    for i in range(count):
        wf_id = f"{SCENARIOS[scenario]['id_prefix']}-{run}-{i}"
        if scenario == "gpu":
            await client.start_workflow(
                GpuTrainingWorkflow.run,
                GpuTrainingInput(
                    model_id=f"model-{run}-{i}",
                    capacity=caps[GPU_RESOURCE],
                ),
                id=wf_id,
                task_queue=TEMPORAL_TASK_QUEUE,
            )
        elif scenario == "ingest":
            await client.start_workflow(
                IngestDocumentWorkflow.run,
                IngestDocumentInput(
                    doc_id=f"doc-{run}-{i}",
                    ocr_capacity=caps[OCR_RESOURCE],
                    embed_capacity=caps[EMBED_RESOURCE],
                    store_capacity=caps[STORE_RESOURCE],
                ),
                id=wf_id,
                task_queue=TEMPORAL_TASK_QUEUE,
            )
        elif scenario == "gate":
            await client.start_workflow(
                ThrottledGateWorkflow.run,
                GateInput(
                    job_id=f"job-{run}-{i}",
                    capacity=caps[GATE_RESOURCE],
                ),
                id=wf_id,
                task_queue=TEMPORAL_TASK_QUEUE,
            )
        started.append(wf_id)
    return {"scenario": scenario, "count": len(started), "ids": started}


async def _slot_status(
    client: Client, resource: str, slots: list[str]
) -> dict[str, Any]:
    held: dict[str, dict[str, Any]] = {}
    for slot in slots:
        wf_id = permit_workflow_id(resource, slot)
        try:
            handle = client.get_workflow_handle(wf_id)
            desc = await handle.describe()
            if desc.status == WorkflowExecutionStatus.RUNNING:
                held[slot] = {
                    "workflow_id": wf_id,
                    "run_id": desc.run_id,
                    "started_at": (
                        desc.start_time.isoformat() if desc.start_time else None
                    ),
                }
        except Exception:
            pass
    return {
        "resource": resource,
        "capacity": len(slots),
        "slots": [
            {"slot": slot, "held_by": held.get(slot)}
            for slot in slots
        ],
        "held_count": len(held),
        "free_count": len(slots) - len(held),
    }


@app.get("/api/status/{resource}")
async def get_status(resource: str, request: Request) -> dict[str, Any]:
    caps = _capacities(request)
    if resource not in caps:
        raise HTTPException(404, f"unknown resource: {resource}")
    slots = _slot_names(resource, caps[resource])
    return await _slot_status(_client(request), resource, slots)


@app.get("/api/recent/{scenario}")
async def recent_runs(
    scenario: str, request: Request, limit: int = 20
) -> dict[str, Any]:
    if scenario not in SCENARIOS:
        raise HTTPException(404, f"unknown scenario: {scenario}")
    s = SCENARIOS[scenario]
    client = _client(request)
    items: list[dict[str, Any]] = []
    query = f'WorkflowType = "{s["wf_type_name"]}"'
    try:
        async for w in client.list_workflows(query, limit=limit * 2):
            items.append(
                {
                    "id": w.id,
                    "run_id": w.run_id,
                    "status": w.status.name if w.status else "UNKNOWN",
                    "start_time": (
                        w.start_time.astimezone(timezone.utc).isoformat()
                        if w.start_time
                        else None
                    ),
                    "close_time": (
                        w.close_time.astimezone(timezone.utc).isoformat()
                        if w.close_time
                        else None
                    ),
                }
            )
    except Exception as e:
        logger.warning("list_workflows failed: %s", e)

    items.sort(key=lambda i: i["start_time"] or "", reverse=True)
    items = items[:limit]
    return {"scenario": scenario, "items": items}


@app.get("/api/health")
async def health() -> dict[str, Any]:
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}


def run() -> None:
    import uvicorn

    host = os.environ.get("API_HOST", "127.0.0.1")
    port = int(os.environ.get("API_PORT", "8000"))
    uvicorn.run("api.main:app", host=host, port=port, reload=False, log_level="info")


if __name__ == "__main__":
    run()
