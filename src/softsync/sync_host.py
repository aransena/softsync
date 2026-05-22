from __future__ import annotations

import os
from typing import Any, Mapping, Sequence

from softsync.core import Coordinator, PendingStart, StartSignal


def prepare(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
    reset: bool = False,
) -> dict[str, Any]:
    """Create host-owned sync state without starting or arming the run."""

    return Coordinator(sync_id, root=root, reset=reset).status()


def set_start(
    sync_id: str,
    *,
    delay_s: float | None = None,
    target_ns: int | None = None,
    clock: str = "monotonic_ns",
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
    overwrite: bool = False,
    reset: bool = False,
) -> PendingStart:
    """Arm a scheduled start signal for `sync_id`.

    Use either `delay_s` or `target_ns`. For an immediate signal, call `start(sync_id)`
    so immediate starts are explicit at the call site.
    """

    if delay_s is None and target_ns is None:
        raise ValueError("provide delay_s or target_ns, or call sync_host.start(sync_id)")
    if delay_s is not None and target_ns is not None:
        raise ValueError("provide delay_s or target_ns, not both")

    return Coordinator(sync_id, root=root, reset=reset).set_start(
        delay_s=delay_s,
        clock=clock,
        target_ns=target_ns,
        metadata=metadata,
        overwrite=overwrite,
    )


def start(
    sync_id: str,
    *,
    clock: str = "monotonic_ns",
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
    overwrite: bool = False,
    reset: bool = False,
    nonblocking: bool = False,
) -> StartSignal:
    """Publish the active start signal for `sync_id`.

    If `set_start()` armed a schedule, that schedule is used. Otherwise the
    start signal is immediate.
    """

    return Coordinator(sync_id, root=root, reset=reset).start(
        clock=clock,
        metadata=metadata,
        overwrite=overwrite,
        nonblocking=nonblocking,
    )


def cancel(
    sync_id: str,
    *,
    reason: str | None = None,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    return Coordinator(sync_id, root=root).cancel(
        reason=reason,
        metadata=metadata,
        overwrite=overwrite,
    )


def stop(
    sync_id: str,
    *,
    reason: str | None = None,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    return Coordinator(sync_id, root=root).stop(
        reason=reason,
        metadata=metadata,
        overwrite=overwrite,
    )


def wait_for_clients(
    sync_id: str,
    client_ids: Sequence[str] | None = None,
    *,
    count: int | None = None,
    max_age_ns: int | None = None,
    timeout_s: float | None = None,
    poll_interval_s: float = 0.01,
    root: str | os.PathLike[str] | None = None,
) -> list[dict[str, Any]]:
    return Coordinator(sync_id, root=root).wait_for_clients(
        client_ids,
        count=count,
        max_age_ns=max_age_ns,
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
    )


def get_clients(
    sync_id: str,
    *,
    max_age_ns: int | None = None,
    root: str | os.PathLike[str] | None = None,
) -> list[dict[str, Any]]:
    """Return clients that have reported any known state for `sync_id`.

    `max_age_ns` is in nanoseconds. When provided, stale reports are filtered out.
    """

    return Coordinator(sync_id, root=root, create=False).get_clients(max_age_ns=max_age_ns)


def status(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    return Coordinator(sync_id, root=root, create=False).status()


def reset(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
) -> None:
    Coordinator(sync_id, root=root).reset()


def remove(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
) -> None:
    Coordinator(sync_id, root=root, create=False).remove()
