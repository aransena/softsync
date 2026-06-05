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
pip install dist/softsync-0.2.0-py3-none-any.whl
```

The package exposes the `softsync` Python module and the `softsync` command.
Host lifecycle locking uses POSIX `fcntl`, so Windows is not currently
supported.

## Examples

Runnable host/client examples live in the source repository under `examples/`.
From the repository, run the host in one terminal:

```bash
PYTHONPATH=src python examples/host.py
```

Then run the client in another terminal:

```bash
PYTHONPATH=src python examples/client.py
```

## Core Start Flow

The host prepares a sync ID and defines the next start condition. With no timing
arguments, the start condition is immediate:

```python
sync_host.prepare("experiment-a")
```

Delay after the host has called `start()` and any configured readiness gate has
been satisfied:

```python
sync_host.prepare("experiment-a", delay_ns=500_000_000)
```

Exact monotonic timestamp:

```python
import time

timestamp_ns = time.monotonic_ns() + 500_000_000
sync_host.prepare("experiment-a", timestamp_ns=timestamp_ns)
```

Client readiness can also be configured at prepare time:

```python
sync_host.prepare("experiment-a", wait_for_N_clients=2)
sync_host.prepare("experiment-a", wait_for_clients=["camera-01", "camera-02"])
```

`prepare()` only records the readiness gate. `start()` is the host call that
blocks until the gate is satisfied. While `start()` is waiting, the host creates
`waiting_for_clients.json`.

Clients do not need to wait for `waiting_for_clients.json` before calling
`sync_ready()`. They can report ready any time after the host has prepared the
sync folder. Clients that only want to report ready on host demand can wait for
the request first:

```python
sync_client.wait_for_ready_request("experiment-a")
sync_client.sync_ready("experiment-a", "camera-01")
```

If the host uses a generated sync ID, clients can discover active readiness
requests and choose the matching `sync_id` in their own code:

```python
sync_ids = sync_client.discover_ready_sync_ids()
sync_id = next(sync_id for sync_id in sync_ids if sync_id.startswith("capture-"))
sync_client.wait_for_ready_request(sync_id)
sync_client.sync_ready(sync_id, "camera-01")
```

The host then starts without scheduling arguments:

```python
sync_host.start("experiment-a")
```

`start()` waits for the prepared readiness gate first. It removes
`waiting_for_clients.json` once the gate is satisfied, then satisfies the
prepared start condition and writes `start.json`. The creation of `start.json`
is the go-signal for clients. Clients do not sleep until a future timestamp; the
host only creates `start.json` when client action should begin.

If a `timestamp_ns` start condition is missed before the required clients are
ready, `start()` raises `SoftSyncStateError` and does not publish `start.json`.

Client start wait:

```python
signal = sync_client.wait_for_start("experiment-a", client_id="camera-01")
start_capture()
```

`signal` is a `StartSignal` parsed from `start.json`. On Linux, waiting uses
`inotify` to wake on file changes. If `inotify` is unavailable, it falls back to
polling. Wait-style client calls can be started before the host has prepared the
sync folder; they wait until the folder and target signal appear or until their
timeout expires.

Async client form:

```python
signal = await sync_client.async_wait_for_start("experiment-a", client_id="camera-01")
start_capture()
```

## Client Reports

Ready. Clients can report ready any time after host `prepare()` has created the
sync folder:

```python
sync_client.sync_ready("experiment-a", "camera-01")
```

When a client should only report ready on host demand, it can wait for the
host's active readiness request first:

```python
sync_client.wait_for_ready_request("experiment-a")
sync_client.sync_ready("experiment-a", "camera-01")
```

Active heartbeat. Call this once work is running, then call it repeatedly at
your chosen heartbeat interval so the host can see the client is still alive:

```python
sync_client.sync_active("experiment-a", "camera-01")
```

Done:

```python
sync_client.sync_done("experiment-a", "camera-01")
```

These report calls are non-blocking. They write JSON files containing `sync_id`,
`client_id`, client process identity, pid, boot id, and nanosecond timestamps.
They raise `SoftSyncNotFound` if the host has not already created the sync
folder.

The host can inspect client reports:

```python
clients = sync_host.get_clients("experiment-a")
fresh = sync_host.get_clients("experiment-a", max_age_ns=5_000_000_000)
```

`max_age_ns` filters stale client reports using nanoseconds. Monotonic timestamps
are used when the boot id matches; Unix timestamps are the fallback. Without
`max_age_ns`, returned reports include `age_ns`, so the host can inspect
`client["active"]["age_ns"]` to decide whether a running client has gone stale.

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

Cancel:

```python
sync_host.cancel("experiment-a", reason="setup failed")
```

Clients waiting for start or stop raise `SoftSyncCancelled` when `cancel.json`
appears. A `stop.json` observed before `start.json` also prevents a client start.

`stop()` and `cancel()` write their terminal signal, remove
`waiting_for_clients.json` and the active `start.json` go-signal, clear
ready/active reports, and clear the prepared session run configuration. The
terminal `stop.json` or `cancel.json` remains so clients can observe the reason.
`done/` reports are kept available for clients that report completion after
stop.

Reset run state:

```python
sync_host.reset("experiment-a")
```

`reset()` clears `waiting_for_clients.json`, `start.json`, `stop.json`,
`cancel.json`, and report directories. It keeps the sync folder and returns the
ID to an immediate, unstarted state.

Remove sync folder:

```python
sync_host.remove("experiment-a")
```

`remove()` deletes `/tmp/softsync/<sync_id>/`.

## File Layout

```text
/tmp/softsync/
  experiment-a/
    session.json
    waiting_for_clients.json
    start.json
    stop.json
    cancel.json
    ready/
      camera-01.json
    active/
      camera-01.json
    done/
      camera-01.json
```

Only `start.json` is required for the basic client start contract.
`waiting_for_clients.json` exists only while the host is actively waiting for
ready clients.

## Message Contracts

All JSON messages include `schema`, `sync_id`, hostname, pid, boot id, and
nanosecond timestamps. `sync_id` and `client_id` are path-safe single path
segments: letters, numbers, `.`, `_`, `-`.

Host lifecycle messages are serialized with a per-sync file lock under
`/tmp/softsync/.locks/<sync_id>.lock`. Without overwrite on terminal signals,
lifecycle messages use exclusive atomic creation so concurrent hosts cannot both
successfully publish the same lifecycle file.

- `session.json`: host setup and next-run configuration. Schema
  `softsync.session.v1`. It can contain `start_condition` with schema
  `softsync.start_condition.v1` and type `immediate`, `delay_ns`, or
  `timestamp_ns`. It can also contain `readiness_gate` with schema
  `softsync.readiness_gate.v1` and either `wait_for_N_clients` or
  `wait_for_clients`.
- `waiting_for_clients.json`: active readiness request. Schema
  `softsync.waiting_for_clients.v1`, command `waiting_for_clients`, and the
  prepared `readiness_gate`. It exists only while `start()` is waiting for the
  gate and is removed when the gate is satisfied, reset, stopped, or cancelled.
- `start.json`: active go-signal. Schema `softsync.start.v1`, command `start`,
  `clock`, `target_ns`, `start_at_monotonic_ns`, the prepared condition, and the
  clients that satisfied the readiness gate. It is created only when client work
  should begin.
- `stop.json`: graceful stop signal. Schema `softsync.stop.v1`, command `stop`,
  optional `reason`.
- `cancel.json`: abortive signal. Schema `softsync.cancel.v1`, command
  `cancel`, optional `reason`.

Client report messages are replaceable status reports. Each includes `schema`,
`sync_id`, `client_id`, `state`, timestamps, identity fields, and optional
`metadata`.

- `ready/<client_id>.json`: schema `softsync.ready.v1`, state `ready`.
- `active/<client_id>.json`: schema `softsync.active.v1`, state `active`.
- `done/<client_id>.json`: schema `softsync.done.v1`, state `done`.

Read-only host APIs do not create sync state. `sync_host.get_clients()` and
`sync_host.status()` raise `SoftSyncNotFound` when the sync folder does not
exist. Client report APIs also raise `SoftSyncNotFound` when the sync folder
does not exist. Client wait APIs can wait for a future host-created sync folder,
but they do not create it; only host APIs create sync ID folders.

## CLI

```bash
softsync prepare experiment-a
softsync prepare experiment-a --delay-ns 500000000
softsync prepare experiment-a --timestamp-ns 1234567890
softsync prepare experiment-a --wait-for-n-clients 2
softsync prepare experiment-a --wait-for-client camera-01 --wait-for-client camera-02
softsync start experiment-a
softsync wait-ready experiment-a
softsync discover-ready
softsync wait experiment-a --client-id camera-01
softsync ready experiment-a camera-01
softsync active experiment-a camera-01
softsync done experiment-a camera-01
softsync clients experiment-a --max-age-ns 5000000000
softsync stop experiment-a --reason "capture complete"
softsync reset experiment-a
softsync remove experiment-a
```

## Contract

- Signals live under `/tmp/softsync/<sync_id>/`.
- The host owns sync folder creation via `prepare()`, `start()`, `stop()`,
  `cancel()`, or `reset()`.
- `prepare()` defines the next start condition and optional readiness gate.
- `start()` has no timing/readiness arguments; it blocks until any prepared
  readiness gate is satisfied, then executes the prepared start condition.
- `waiting_for_clients.json` exists only while the host is actively waiting for
  a prepared readiness gate.
- Active host start is always `start.json`.
- Clients treat `start.json` as the go-signal and do not implement start delay
  behavior.
- `start.json`, `stop.json`, `cancel.json`, and report files are written
  atomically.
- Readers must treat a missing `start.json` as "not started yet" unless a
  terminal `stop.json` or `cancel.json` is present.
- The default and only prepared start clock is `monotonic_ns`, intended for one
  machine and one boot.

`softsync` is not a network clock synchronisation system and does not replace
hardware triggers, LSL, PTP, NTP, or ROS middleware. It is a tiny local control
plane for prepared processes that already know how to gate their own work.

## Maintenance Notes

Treat the message contracts above as compatibility boundaries. If changing a
schema, payload field, file name, or state-machine rule, update the README,
payload constructors, exports, CLI behavior, and tests in the same change. Use
`softsync` terminology for new public names. Keep host lifecycle mutations under
the per-sync lifecycle lock, keep non-overwrite lifecycle writes exclusive, keep
read-only APIs free of filesystem side effects, and keep clients from creating
sync ID folders.
