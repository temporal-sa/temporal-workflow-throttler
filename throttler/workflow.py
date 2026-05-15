"""Permit-as-workflow throttler primitives.

Each held permit is its own short-lived ``PermitSlotWorkflow`` execution.
``Semaphore.acquire(...)`` tries to start a slot workflow with
``id_conflict_policy = FAIL`` semantics (via ``start_child_workflow`` +
``WorkflowAlreadyStartedError``); the first caller to start a given slot owns
it. ``release`` is a fire-and-forget signal to that slot.

This pattern avoids the per-resource signal-QPS bottleneck of a central
limiter because signal load distributes across N independent slot workflows.
Temporal's own enforcement of workflow-ID uniqueness is the source of truth
for "is this slot free?", atomic by construction.

The library has **no activities** -- the entire acquire/release dance happens
in pure workflow code. (For very large pools, ``len(slots) > 32``, a future
"list held slots via Visibility" optimisation would be the only thing to
need an activity, since Visibility queries require a ``Client``. Skipped in
v1 because random-probe is fast enough at demo pool sizes.)
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import timedelta
from typing import AsyncIterator, Sequence

from temporalio import workflow
from temporalio.exceptions import FailureError, WorkflowAlreadyStartedError
from temporalio.workflow import ParentClosePolicy

from throttler.config import DEFAULT_BACKOFF, DEFAULT_LEASE, permit_workflow_id


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
      1. Started by ``Semaphore.acquire(...)`` with ``id_conflict_policy=FAIL``.
         If the workflow ID is already running, start fails -> caller tries the
         next slot.
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


class Semaphore:
    """Caller-side helper. Construct inside an ``@workflow.run`` and call
    ``acquire`` to get a slot (lock).

    Two modes (generic vs resource locks):
      - Counter:    ``Semaphore("app-gate", capacity=10)``
      - Named slot: ``Semaphore("gpu-pool", slots=["gpu-0", "gpu-1", ...])``

    Counter mode auto-generates slot names ``"0".."N-1"``. The API is otherwise
    identical and the underlying mechanism is the same.
    """

    def __init__(
        self,
        resource: str,
        *,
        capacity: int | None = None,
        slots: Sequence[str] | None = None,
        task_queue: str | None = None,
    ) -> None:
        if (capacity is None) == (slots is None):
            raise ValueError("Pass exactly one of `capacity` or `slots`.")
        if capacity is not None:
            if capacity < 1:
                raise ValueError("capacity must be >= 1")
            slots = [str(i) for i in range(capacity)]
        assert slots is not None
        if not slots:
            raise ValueError("slots must be non-empty")
        if len(set(slots)) != len(slots):
            raise ValueError("slot names must be unique")

        self._resource = resource
        self._slots: list[str] = list(slots)
        self._task_queue = task_queue

    @property
    def resource(self) -> str:
        return self._resource

    @property
    def slots(self) -> list[str]:
        return list(self._slots)

    @asynccontextmanager
    async def acquire(
        self,
        *,
        lease: timedelta = DEFAULT_LEASE,
        backoff: timedelta = DEFAULT_BACKOFF,
    ) -> AsyncIterator[str]:
        """Take a permit. Yields the slot name. Releases on exit.

        Probes slots in deterministic-random order and falls through on
        ``WorkflowAlreadyStartedError``. If all slots are held, sleeps for
        ``backoff`` and retries. This is could be more sophisticated or tailored
        to use case.
        """
        slot, run_id = await self._acquire_one(lease=lease, backoff=backoff)
        try:
            yield slot
        finally:
            await self._release(slot, run_id)

    async def _acquire_one(
        self, *, lease: timedelta, backoff: timedelta
    ) -> tuple[str, str]:
        """Returns ``(slot_name, run_id)``. The ``run_id`` is the specific
        execution we just started so that ``_release`` can target *that* run
        rather than whichever execution happens to hold the slot's workflow id
        at release time. Without this pin, a release issued after our lease
        expired could silently revoke a subsequent acquirer's permit."""
        rng = workflow.random()
        lease_seconds = lease.total_seconds()
        while True:
            order = list(self._slots)
            rng.shuffle(order)
            for slot in order:
                wf_id = permit_workflow_id(self._resource, slot)
                try:
                    handle = await workflow.start_child_workflow(
                        PermitSlotWorkflow.run,
                        PermitSlotInput(
                            resource=self._resource,
                            slot=slot,
                            lease_seconds=lease_seconds,
                        ),
                        id=wf_id,
                        task_queue=self._task_queue,
                        parent_close_policy=ParentClosePolicy.ABANDON,
                    )
                    run_id = handle.first_execution_run_id
                    assert run_id is not None, (
                        "start_child_workflow returned without a run_id"
                    )
                    return slot, run_id
                except WorkflowAlreadyStartedError:
                    continue
            await workflow.sleep(backoff)

    async def _release(self, slot: str, run_id: str) -> None:
        """Send the release signal pinned to ``run_id`` so it cannot land on a
        later acquirer's execution. If the targeted run already finished
        (e.g. lease auto-expiry), the signal raises a ``FailureError`` which
        we treat as a no-op since the slot is already free."""
        wf_id = permit_workflow_id(self._resource, slot)
        handle = workflow.get_external_workflow_handle(wf_id, run_id=run_id)
        try:
            await handle.signal("release")
        except FailureError as e:
            workflow.logger.info(
                "release signal target already finished (lease likely expired)",
                extra={
                    "resource": self._resource,
                    "slot": slot,
                    "run_id": run_id,
                    "error": str(e),
                },
            )
