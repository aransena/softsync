from __future__ import annotations

import asyncio
import os
from typing import Any, Mapping

from softsync.core import Client, StartSignal


def sync_watch(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    fallback_poll_interval_s: float = 0.005,
) -> StartSignal:
    """Watch `sync_id`, then return when its start boundary arrives."""

    return Client(sync_id, client_id, root=root).sync_watch(
        timeout_s=timeout_s,
        poll_interval_s=fallback_poll_interval_s,
    )


def watch_start(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    fallback_poll_interval_s: float = 0.005,
) -> StartSignal:
    """Watch until `start.json` exists, without sleeping to its target time."""

    return Client(sync_id, client_id, root=root).watch_start(
        timeout_s=timeout_s,
        poll_interval_s=fallback_poll_interval_s,
    )


async def async_watch_start(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    fallback_poll_interval_s: float = 0.005,
) -> StartSignal:
    return await asyncio.to_thread(
        watch_start,
        sync_id,
        client_id=client_id,
        root=root,
        timeout_s=timeout_s,
        fallback_poll_interval_s=fallback_poll_interval_s,
    )


async def async_sync_watch(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    fallback_poll_interval_s: float = 0.005,
) -> StartSignal:
    return await asyncio.to_thread(
        sync_watch,
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
    """Notify the host that this client is alive and monitoring `sync_id`."""

    return Client(sync_id, client_id, root=root).sync_active(metadata=metadata)


def report_start(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Report that this client has started work."""

    return Client(sync_id, client_id, root=root).report_start(metadata=metadata)


def sync_done(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return Client(sync_id, client_id, root=root).sync_done(metadata=metadata)
