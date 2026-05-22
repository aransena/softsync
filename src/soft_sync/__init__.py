__version__ = "0.1.0"

from soft_sync.core import (
    DEFAULT_ROOT,
    ACTIVE_SCHEMA,
    CANCEL_SCHEMA,
    CLIENT_STARTED_SCHEMA,
    DONE_SCHEMA,
    PENDING_START_SCHEMA,
    READY_SCHEMA,
    SESSION_SCHEMA,
    START_SCHEMA,
    STOP_SCHEMA,
    Client,
    Coordinator,
    PendingStart,
    SoftSyncCancelled,
    SoftSyncClockMismatch,
    SoftSyncError,
    SoftSyncNotFound,
    SoftSyncStateError,
    SoftSyncTimeout,
    StartSignal,
    client,
    coordinator,
    local_boot_id,
    sync_path,
)
from . import sync_client, sync_host

set_start = sync_host.set_start
start = sync_host.start
stop = sync_host.stop
remove = sync_host.remove
get_clients = sync_host.get_clients
wait_for_clients = sync_host.wait_for_clients
sync_ready = sync_client.sync_ready
sync_active = sync_client.sync_active
sync_watch = sync_client.sync_watch
watch_start = sync_client.watch_start
watch_stop = sync_client.watch_stop
async_sync_watch = sync_client.async_sync_watch
async_watch_start = sync_client.async_watch_start
async_watch_stop = sync_client.async_watch_stop
report_start = sync_client.report_start
sync_done = sync_client.sync_done

__all__ = [
    "__version__",
    "DEFAULT_ROOT",
    "ACTIVE_SCHEMA",
    "CANCEL_SCHEMA",
    "CLIENT_STARTED_SCHEMA",
    "DONE_SCHEMA",
    "PENDING_START_SCHEMA",
    "READY_SCHEMA",
    "SESSION_SCHEMA",
    "START_SCHEMA",
    "STOP_SCHEMA",
    "async_sync_watch",
    "async_watch_start",
    "async_watch_stop",
    "Client",
    "Coordinator",
    "PendingStart",
    "SoftSyncCancelled",
    "SoftSyncClockMismatch",
    "SoftSyncError",
    "SoftSyncNotFound",
    "SoftSyncStateError",
    "SoftSyncTimeout",
    "StartSignal",
    "client",
    "coordinator",
    "get_clients",
    "local_boot_id",
    "remove",
    "report_start",
    "set_start",
    "start",
    "stop",
    "sync_active",
    "sync_client",
    "sync_done",
    "sync_host",
    "sync_path",
    "sync_ready",
    "sync_watch",
    "wait_for_clients",
    "watch_start",
    "watch_stop",
]
