from __future__ import annotations

import asyncio
import os
from typing import Any, Mapping

from softsync.core import (
    Client,
    StartSignal,
    discover_ready_sync_ids as _discover_ready_sync_ids,
    wait_for_ready_request as _wait_for_ready_request,
)


def discover_ready_sync_ids(
    *,
    root: str | os.PathLike[str] | None = None,
) -> list[str]:
    """Return sync IDs for hosts currently requesting ready clients."""

    return _discover_ready_sync_ids(root=root)


def wait_for_ready_request(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    fallback_poll_interval_s: float = 0.005,
) -> dict[str, Any]:
    """Wait until the host is actively waiting for ready clients."""

    return _wait_for_ready_request(
        sync_id,
        root=root,
        timeout_s=timeout_s,
        poll_interval_s=fallback_poll_interval_s,
    )


async def async_wait_for_ready_request(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    fallback_poll_interval_s: float = 0.005,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        wait_for_ready_request,
        sync_id,
        root=root,
        timeout_s=timeout_s,
        fallback_poll_interval_s=fallback_poll_interval_s,
    )


def wait_for_start(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    fallback_poll_interval_s: float = 0.005,
) -> StartSignal:
    """Wait until `start.json` exists, then return immediately."""

    return Client(sync_id, client_id, root=root).wait_for_start(
        timeout_s=timeout_s,
        poll_interval_s=fallback_poll_interval_s,
    )


async def async_wait_for_start(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    fallback_poll_interval_s: float = 0.005,
) -> StartSignal:
    return await asyncio.to_thread(
        wait_for_start,
        sync_id,
        client_id=client_id,
        root=root,
        timeout_s=timeout_s,
        fallback_poll_interval_s=fallback_poll_interval_s,
    )


def watch_stop(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    fallback_poll_interval_s: float = 0.005,
) -> dict[str, Any]:
    return Client(sync_id, client_id, root=root).watch_stop(
        timeout_s=timeout_s,
        poll_interval_s=fallback_poll_interval_s,
    )


async def async_watch_stop(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    fallback_poll_interval_s: float = 0.005,
) -> dict[str, Any]:
    return await asyncio.to_thread(
        watch_stop,
        sync_id,
        client_id=client_id,
        root=root,
        timeout_s=timeout_s,
        fallback_poll_interval_s=fallback_poll_interval_s,
    )


def sync_ready(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Notify the host that this client is ready for `sync_id`."""

    return Client(sync_id, client_id, root=root).sync_ready(metadata=metadata)


def sync_active(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Refresh this client's active heartbeat for `sync_id`."""

    return Client(sync_id, client_id, root=root).sync_active(metadata=metadata)


def sync_done(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return Client(sync_id, client_id, root=root).sync_done(metadata=metadata)
