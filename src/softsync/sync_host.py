from __future__ import annotations

import os
from typing import Any, Mapping, Sequence

from softsync.core import Coordinator, StartSignal, _normalise_prepare_args


def prepare(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
    reset: bool = False,
    delay_ns: int | None = None,
    timestamp_ns: int | None = None,
    wait_for_N_clients: int | None = None,
    wait_for_clients: Sequence[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Create host-owned sync state and configure the next start condition."""

    _normalise_prepare_args(
        delay_ns=delay_ns,
        timestamp_ns=timestamp_ns,
        wait_for_N_clients=wait_for_N_clients,
        wait_for_clients=wait_for_clients,
    )
    return Coordinator(sync_id, root=root, reset=reset).prepare(
        delay_ns=delay_ns,
        timestamp_ns=timestamp_ns,
        wait_for_N_clients=wait_for_N_clients,
        wait_for_clients=wait_for_clients,
        metadata=metadata,
    )


def start(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
    verbose: bool = False,
) -> StartSignal:
    """Wait for prepared conditions, then publish the active start signal."""

    return Coordinator(sync_id, root=root).start(verbose=verbose)


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


def get_clients(
    sync_id: str,
    *,
    max_age_ns: int | None = None,
    root: str | os.PathLike[str] | None = None,
) -> list[dict[str, Any]]:
    """Return clients that have reported any known state for `sync_id`."""

    return Coordinator(sync_id, root=root, create=False).get_clients(max_age_ns=max_age_ns)


def status(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    return Coordinator(sync_id, root=root, create=False).status()


def validate(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
) -> dict[str, Any]:
    return Coordinator(sync_id, root=root, create=False).validate()


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
