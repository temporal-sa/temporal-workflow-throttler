"""``PermitSlotWorkflow`` definition.

Each held permit is its own short-lived ``PermitSlotWorkflow`` execution.
:class:`throttler.semaphore.Semaphore` (the caller-side helper) starts one of
these per acquired slot via ``start_child_workflow`` with
``parent_close_policy=ABANDON``. Temporal's enforcement of workflow-id
uniqueness is the source of truth for "is this slot free?", atomic by
construction; an attempt to start a second permit with the same id fails with
``WorkflowAlreadyStartedError`` and the caller probes the next slot.

The library has **no activities** -- the entire acquire/release dance happens
in pure workflow code.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta

from temporalio import workflow


@dataclass
class PermitSlotInput:
    """Args for ``PermitSlotWorkflow``. Encoded as a dataclass so future fields
    (e.g. holder identity, search-attribute hints) can be added without
    breaking history."""

    resource: str
    slot: str
    lease_seconds: float


@workflow.defn(name="PermitSlotWorkflow")
class PermitSlotWorkflow:
    """A single held permit.

    Lifecycle:
      1. Started by ``Semaphore.acquire(...)`` via ``start_child_workflow``.
         If the workflow id is already running, the start fails with
         ``WorkflowAlreadyStartedError`` and the caller tries the next slot.
      2. While running, the slot is held.
      3. Exits when either:
           - a ``release`` signal arrives (normal release), or
           - the lease timeout fires (orphan recovery if the holder dies).
    """

    def __init__(self) -> None:
        self._released: bool = False

    @workflow.signal(name="release")
    def release(self) -> None:
        self._released = True

    @workflow.query(name="held")
    def held(self) -> bool:
        return not self._released

    @workflow.run
    async def run(self, input: PermitSlotInput) -> str:
        workflow.logger.info(
            "permit acquired",
            extra={"resource": input.resource, "slot": input.slot},
        )
        try:
            await workflow.wait_condition(
                lambda: self._released,
                timeout=timedelta(seconds=input.lease_seconds),
            )
            return "released"
        except TimeoutError:
            workflow.logger.warning(
                "permit lease expired - auto-releasing slot",
                extra={"resource": input.resource, "slot": input.slot},
            )
            return "lease_expired"
