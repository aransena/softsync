# softsync

`softsync` is a small single-machine coordination contract for services that can
prepare first and start later. A host publishes named lifecycle signals under
`/tmp/softsync/<sync_id>/`, and clients watch or report state for that same
`sync_id`.

Use role-specific imports:

```python
from softsync import sync_host
from softsync import sync_client
```

There is no default sync ID. Every call requires an explicit `sync_id` to reduce
accidental cross-experiment collisions.

## Installation

Install from the project folder:

```bash
pip install /path/to/softsync
```

Install from the built wheel:

```bash
pip install dist/softsync-0.1.0-py3-none-any.whl
```

The package exposes the `softsync` Python module and the `softsync` command.

## Core Start Flow

Host setup without starting:

```python
sync_host.prepare("experiment-a")
```

Use `prepare()` when clients need to start watching or report readiness before
any start, stop, or cancel signal exists. Clients never create sync ID folders.

Immediate host start:

```python
sync_host.start("experiment-a")
```

Scheduled host start:

```python
sync_host.set_start("experiment-a", delay_s=0.5)
sync_host.start("experiment-a")
```

Exact target timestamp:

```python
import time

target_ns = time.monotonic_ns() + 500_000_000
sync_host.set_start("experiment-a", target_ns=target_ns)
sync_host.start("experiment-a")
```

`set_start()` only writes `pending_start.json`. It does not activate clients.
`start()` writes `start.json`, which is the active signal clients observe.

`sync_host.start()` is non-blocking. If a delay or target timestamp was armed,
`start()` writes `start.json` immediately with that future target and returns.
The creation of `start.json` releases the schedule to clients; the target
timestamp is the work boundary. Clients that use `sync_watch()` block until that
target arrives, while clients that use `watch_start()` can forward the target
into another prepared API.

Client that starts its own work:

```python
sync_client.sync_watch("experiment-a", client_id="camera-01")
start_capture()
```

`sync_watch()` blocks until `start.json` exists, then waits until the target
start time has arrived. On Linux it uses `inotify` to wake on file changes. If
`inotify` is unavailable, it falls back to polling.

Client that forwards the target into another prepared API:

```python
signal = sync_client.watch_start("experiment-a", client_id="device-01")
device.start_at_ns(signal.start_at_ns)
```

`signal` is a `StartSignal` parsed from `start.json`. Use this pattern when your
client is only passing the target timestamp into another service that handles
its own gate.

Async client form:

```python
signal = await sync_client.async_sync_watch("experiment-a", client_id="camera-01")
start_capture()
```

The async helpers run the same file watcher in a worker thread via `asyncio`.

## Client Reports

Ready:

```python
sync_client.sync_ready("experiment-a", "camera-01")
```

Active heartbeat:

```python
sync_client.sync_active("experiment-a", "camera-01")
```

Started report:

```python
sync_client.report_start("experiment-a", "camera-01")
```

Done:

```python
sync_client.sync_done("experiment-a", "camera-01")
```

These calls are non-blocking. They write JSON files containing `sync_id`,
`client_id`, host identity, pid, boot id, and nanosecond timestamps. They raise
`SoftSyncNotFound` if the host has not already created the sync folder.

The host can inspect client reports:

```python
clients = sync_host.get_clients("experiment-a")
fresh = sync_host.get_clients("experiment-a", max_age_ns=5_000_000_000)
```

`max_age_ns` filters stale client reports using nanoseconds. Monotonic timestamps
are used when the boot id matches; Unix timestamps are the fallback.

The host can optionally wait for ready clients:

```python
sync_host.prepare("experiment-a")
sync_host.wait_for_clients("experiment-a", count=2, timeout_s=10)
sync_host.start("experiment-a")
```

`wait_for_clients()` is blocking and currently uses short-interval polling.

## Stop, Cancel, Reset, Remove

Coordinated stop:

```python
sync_host.stop("experiment-a", reason="capture complete")
```

Client waiting for stop:

```python
stop_signal = sync_client.watch_stop("experiment-a", client_id="camera-01")
stop_capture()
```

`watch_stop()` is blocking and uses the same event-driven Linux `inotify` path
as start watching, with polling fallback.

Cancel:

```python
sync_host.cancel("experiment-a", reason="setup failed")
```

Clients waiting for start or stop raise `SoftSyncCancelled` when `cancel.json`
appears.

Reset run state:

```python
sync_host.reset("experiment-a")
```

`reset()` clears `pending_start.json`, `start.json`, `stop.json`, `cancel.json`,
and the `ready/`, `active/`, `started/`, and `done/` report directories. It keeps
the sync folder and returns the ID to an unstarted state.

Remove sync folder:

```python
sync_host.remove("experiment-a")
```

`remove()` deletes `/tmp/softsync/<sync_id>/`.

## Timing Notes

`sync_watch()` uses `StartSignal.sleep_until_start()`. The wait loop sleeps in
coarse chunks of up to 50 ms until it is near the target, then uses 100 us sleeps,
then a very short tight loop for the last sub-500 us.

The sleep interval does not define the target timestamp, but OS scheduling jitter
can still affect when Python code resumes. `report_start()` lets the host inspect
client start deltas after acquisition begins:

```python
sync_client.sync_watch("experiment-a", client_id="camera-01")
sync_client.report_start("experiment-a", "camera-01")
start_capture()
```

## File Layout

```text
/tmp/softsync/
  experiment-a/
    session.json
    pending_start.json
    start.json
    stop.json
    cancel.json
    ready/
      camera-01.json
    active/
      camera-01.json
    started/
      camera-01.json
    done/
      camera-01.json
```

Only `start.json` is required for the basic client start contract.

## Message Contracts

All JSON messages include `schema`, `sync_id`, hostname, pid, boot id, and
nanosecond timestamps. `sync_id` and `client_id` are path-safe single path
segments: letters, numbers, `.`, `_`, `-`.

Host lifecycle messages are serialized with a per-sync file lock under
`/tmp/softsync/.locks/<sync_id>.lock`. Without `overwrite=True`, lifecycle
messages are created with exclusive atomic creation so concurrent hosts cannot
both successfully publish the same lifecycle file.

- `session.json`: host setup record. Schema `softsync.session.v1`.
- `pending_start.json`: armed but inactive start. Schema
  `softsync.pending_start.v1`, command `set_start`, `clock`, and exactly one of
  `delay_s`/`delay_ns` or `target_ns`.
- `start.json`: active start signal. Schema `softsync.start.v1`, command
  `start`, `clock`, `target_ns`, `armed`, and the matching
  `start_at_monotonic_ns` or `start_at_unix_ns`.
- `stop.json`: graceful stop signal. Schema `softsync.stop.v1`, command
  `stop`, optional `reason`.
- `cancel.json`: abortive signal. Schema `softsync.cancel.v1`, command
  `cancel`, optional `reason`. It may be written before or after `start.json`,
  but not after `stop.json` unless `overwrite=True`.

Client report messages are replaceable status reports. Each includes `schema`,
`sync_id`, `client_id`, `state`, timestamps, identity fields, and optional
`metadata`.

- `ready/<client_id>.json`: schema `softsync.ready.v1`, state `ready`.
- `active/<client_id>.json`: schema `softsync.active.v1`, state `active`.
- `started/<client_id>.json`: schema `softsync.client_started.v1`, state
  `started`.
- `done/<client_id>.json`: schema `softsync.done.v1`, state `done`.

Read-only host APIs do not create sync state. `sync_host.get_clients()` and
`sync_host.status()` raise `SoftSyncNotFound` when the sync folder does not
exist. Client APIs also raise `SoftSyncNotFound` when the sync folder does not
exist; only host APIs create sync ID folders.

## CLI

```bash
softsync prepare experiment-a
softsync set-start experiment-a --delay 0.5
softsync start experiment-a
softsync wait experiment-a --client-id camera-01
softsync ready experiment-a camera-01
softsync active experiment-a camera-01
softsync report-start experiment-a camera-01
softsync clients experiment-a --max-age-ns 5000000000
softsync stop experiment-a --reason "capture complete"
softsync reset experiment-a
softsync remove experiment-a
```

## Contract

- Signals live under `/tmp/softsync/<sync_id>/`.
- The host owns sync folder creation via `prepare()`, `set_start()`, `start()`,
  `wait_for_clients()`, `stop()`, `cancel()`, or `reset()`.
- `sync_id` and `client_id` are path-safe names: letters, numbers, `.`, `_`, `-`.
- Active host start is always `start.json`.
- `pending_start.json` is host-side state and must not activate clients.
- `start.json`, `stop.json`, `cancel.json`, and report files are written atomically.
- Readers must treat a missing `start.json` as "not started yet".
- The default clock is `monotonic_ns`, intended for one machine and one boot.
- `unix_ns` is available for applications that intentionally want wall-clock boundaries.

`softsync` is not a network clock synchronisation system and does not replace
hardware triggers, LSL, PTP, NTP, or ROS middleware. It is a tiny local control
plane for prepared processes that already know how to gate their own work.

## Future Agent Notes

Treat the message contracts above as compatibility boundaries. If changing a
schema, payload field, file name, or state-machine rule, update the README,
payload constructors, exports, CLI behavior, and tests in the same change. Use
`softsync` terminology for new public names. Keep host lifecycle mutations under
the per-sync lifecycle lock, keep non-overwrite lifecycle writes exclusive, keep
read-only APIs free of filesystem side effects, and keep clients from creating
sync ID folders.
