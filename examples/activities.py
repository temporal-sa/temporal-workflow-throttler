"""Demo activities -- intentionally trivial sleep simulations so the
throttling behaviour, not the activity work, is what the UI shows.

All activities sleep for a configurable duration (default 10s) so that
slot occupancy is clearly visible in the UI without finishing in a blink.
"""

from __future__ import annotations

import asyncio

from temporalio import activity

DEFAULT_HOLD_SECONDS: float = 10.0


@activity.defn(name="simulate_work")
async def simulate_work(duration_seconds: float, label: str) -> str:
    activity.logger.info("starting work: %s for %.1fs", label, duration_seconds)
    await asyncio.sleep(duration_seconds)
    activity.logger.info("finished work: %s", label)
    return f"{label}: done in {duration_seconds:.1f}s"


@activity.defn(name="run_training")
async def run_training(model_id: str, gpu_slot: str) -> str:
    duration = DEFAULT_HOLD_SECONDS
    activity.logger.info(
        "training model=%s on %s for %.1fs", model_id, gpu_slot, duration
    )
    await asyncio.sleep(duration)
    return f"trained model={model_id} on {gpu_slot} ({duration:.1f}s)"


@activity.defn(name="do_ocr")
async def do_ocr(doc_id: str) -> str:
    duration = DEFAULT_HOLD_SECONDS
    await asyncio.sleep(duration)
    return f"ocr({doc_id})"


@activity.defn(name="do_embed")
async def do_embed(doc_id: str) -> str:
    duration = DEFAULT_HOLD_SECONDS
    await asyncio.sleep(duration)
    return f"embed({doc_id})"


@activity.defn(name="do_store")
async def do_store(doc_id: str) -> str:
    duration = DEFAULT_HOLD_SECONDS
    await asyncio.sleep(duration)
    return f"store({doc_id})"


ALL_ACTIVITIES = [
    simulate_work,
    run_training,
    do_ocr,
    do_embed,
    do_store,
]
