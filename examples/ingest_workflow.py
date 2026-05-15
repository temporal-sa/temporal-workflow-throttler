"""RAG-style ingest pipeline scenario.

Demonstrates *multiple* semaphores on the same workflow, each protecting a
different downstream resource (OCR pool, embedding API, vector store). Each
stage waits its turn at the appropriate gate without blocking the others.

Capacities for each pool are passed in via the workflow input so the UI can
tune them at run-time.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow

from throttler.semaphore import Semaphore

OCR_RESOURCE = "ocr-pool"
EMBED_RESOURCE = "embed-pool"
STORE_RESOURCE = "vector-store"

DEFAULT_OCR_CAPACITY = 4
DEFAULT_EMBED_CAPACITY = 6
DEFAULT_STORE_CAPACITY = 3


@dataclass
class IngestDocumentInput:
    doc_id: str
    ocr_capacity: int = DEFAULT_OCR_CAPACITY
    embed_capacity: int = DEFAULT_EMBED_CAPACITY
    store_capacity: int = DEFAULT_STORE_CAPACITY


@workflow.defn(name="IngestDocumentWorkflow")
class IngestDocumentWorkflow:
    @workflow.run
    async def run(self, input: IngestDocumentInput) -> str:
        ocr = Semaphore(OCR_RESOURCE, capacity=input.ocr_capacity)
        embed = Semaphore(EMBED_RESOURCE, capacity=input.embed_capacity)
        store = Semaphore(STORE_RESOURCE, capacity=input.store_capacity)

        async with ocr.acquire(lease=timedelta(minutes=5)) as slot:
            workflow.logger.info(
                "ocr acquired", extra={"doc_id": input.doc_id, "slot": slot}
            )
            ocr_out = await workflow.execute_activity(
                "do_ocr",
                input.doc_id,
                start_to_close_timeout=timedelta(minutes=2),
            )

        async with embed.acquire(lease=timedelta(minutes=5)) as slot:
            workflow.logger.info(
                "embed acquired", extra={"doc_id": input.doc_id, "slot": slot}
            )
            embed_out = await workflow.execute_activity(
                "do_embed",
                input.doc_id,
                start_to_close_timeout=timedelta(minutes=2),
            )

        async with store.acquire(lease=timedelta(minutes=5)) as slot:
            workflow.logger.info(
                "store acquired", extra={"doc_id": input.doc_id, "slot": slot}
            )
            store_out = await workflow.execute_activity(
                "do_store",
                input.doc_id,
                start_to_close_timeout=timedelta(minutes=2),
            )

        return f"{ocr_out} -> {embed_out} -> {store_out}"
