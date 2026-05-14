"""Business-logic tests for ``IngestDocumentWorkflow``.

Verifies the pipeline wiring:
  * Each of the three stages (OCR, Embed, Store) is called with the doc_id.
  * Stages execute in order.
  * The workflow returns the chained result string.

Activities are mocked to instant returns; we trust the Semaphore for
concurrency correctness.
"""

from __future__ import annotations

from temporalio import activity
from temporalio.worker import Worker

from examples.ingest_workflow import IngestDocumentInput, IngestDocumentWorkflow
from tests.conftest import with_env
from throttler.workflow import PermitSlotWorkflow

TASK_QUEUE = "throttler-tq-ingest-scenario-test"


async def test_ingest_workflow_runs_three_stages_in_order() -> None:
    call_log: list[tuple[str, str]] = []

    @activity.defn(name="do_ocr")
    async def mock_ocr(doc_id: str) -> str:
        call_log.append(("ocr", doc_id))
        return f"ocr({doc_id})"

    @activity.defn(name="do_embed")
    async def mock_embed(doc_id: str) -> str:
        call_log.append(("embed", doc_id))
        return f"embed({doc_id})"

    @activity.defn(name="do_store")
    async def mock_store(doc_id: str) -> str:
        call_log.append(("store", doc_id))
        return f"store({doc_id})"

    async with with_env() as env:
        async with Worker(
            env.client,
            task_queue=TASK_QUEUE,
            workflows=[IngestDocumentWorkflow, PermitSlotWorkflow],
            activities=[mock_ocr, mock_embed, mock_store],
        ):
            result = await env.client.execute_workflow(
                IngestDocumentWorkflow.run,
                IngestDocumentInput(doc_id="d1"),
                id="ingest-test-1",
                task_queue=TASK_QUEUE,
            )

            assert call_log == [("ocr", "d1"), ("embed", "d1"), ("store", "d1")]
            assert result == "ocr(d1) -> embed(d1) -> store(d1)"
