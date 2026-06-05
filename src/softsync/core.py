from __future__ import annotations

import json
import os
import re
import select
import shutil
import socket
import sys
import time
import uuid
import ctypes
import ctypes.util
from dataclasses import dataclass
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, Mapping, Sequence

try:
    import fcntl
except ImportError:  # pragma: no cover - exercised only on non-Unix platforms.
    fcntl = None

DEFAULT_ROOT = Path("/tmp/softsync")

START_SCHEMA = "softsync.start.v1"
START_CONDITION_SCHEMA = "softsync.start_condition.v1"
READINESS_GATE_SCHEMA = "softsync.readiness_gate.v1"
WAITING_FOR_CLIENTS_SCHEMA = "softsync.waiting_for_clients.v1"
STOP_SCHEMA = "softsync.stop.v1"
CANCEL_SCHEMA = "softsync.cancel.v1"
READY_SCHEMA = "softsync.ready.v1"
ACTIVE_SCHEMA = "softsync.active.v1"
DONE_SCHEMA = "softsync.done.v1"
SESSION_SCHEMA = "softsync.session.v1"

_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$")

_IN_CREATE = 0x00000100
_IN_CLOSE_WRITE = 0x00000008
_IN_MOVED_TO = 0x00000080
_IN_ATTRIB = 0x00000004
_IN_DELETE = 0x00000200
_IN_Q_OVERFLOW = 0x00004000
_IN_CLOEXEC = 0o2000000

_COARSE_SLEEP_MAX_S = 0.05
_COARSE_SLEEP_SLACK_NS = 2_000_000
_FINE_SLEEP_THRESHOLD_NS = 500_000


class SoftSyncError(RuntimeError):
    """Base exception for softsync failures."""


class SoftSyncTimeout(SoftSyncError):
    """Raised when a wait operation times out."""


class SoftSyncCancelled(SoftSyncError):
    """Raised when a client observes terminal cancellation before completion."""

    def __init__(self, payload: Mapping[str, Any]) -> None:
        self.payload = dict(payload)
        super().__init__(str(self.payload.get("reason") or "soft sync cancelled"))


class SoftSyncClockMismatch(SoftSyncError):
    """Raised when a monotonic flag is not from the current local boot."""


class SoftSyncStateError(SoftSyncError):
    """Raised when a flag cannot be written because another terminal flag exists."""


class SoftSyncNotFound(SoftSyncError):
    """Raised when a requested sync ID has no on-disk state."""


def validate_name(value: str, *, field: str) -> str:
    """Validate a sync or client name that maps to a local file path segment."""

    if not isinstance(value, str) or not _NAME_RE.match(value):
        raise ValueError(
            f"{field} must match {_NAME_RE.pattern!r}; use letters, numbers, '.', '_' or '-'"
        )
    return value


def sync_path(sync_id: str, *, root: str | os.PathLike[str] | None = None) -> Path:
    """Return the standard directory for one named sync ID."""

    return Path(root or DEFAULT_ROOT) / validate_name(sync_id, field="sync_id")


def local_boot_id() -> str | None:
    """Return the Linux boot id when available."""

    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except OSError:
        return None


def _identity() -> dict[str, Any]:
    return {
        "hostname": socket.gethostname(),
        "pid": os.getpid(),
        "boot_id": local_boot_id(),
    }


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(path, os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _atomic_write_json(
    path: Path,
    payload: Mapping[str, Any],
    *,
    create_parents: bool = True,
    overwrite: bool = True,
) -> None:
    """Write JSON so readers see either the old file or the complete new file."""

    try:
        path.parent.mkdir(parents=create_parents, exist_ok=True)
    except FileNotFoundError as exc:
        raise SoftSyncNotFound(f"{path.parent.parent} does not exist") from exc
    tmp_path = path.with_name(f".{path.name}.{os.getpid()}.{uuid.uuid4().hex}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8") as handle:
            json.dump(dict(payload), handle, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if overwrite:
            os.replace(tmp_path, path)
        else:
            try:
                os.link(tmp_path, path)
            except FileExistsError as exc:
                raise SoftSyncStateError(f"{path} already exists") from exc
        _fsync_dir(path.parent)
    finally:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise SoftSyncError(f"{path} did not contain a JSON object")
    return payload


def _read_session(session_file: Path) -> dict[str, Any]:
    session = _read_json(session_file)
    if session.get("schema") != SESSION_SCHEMA:
        raise SoftSyncError(f"{session_file} is not a {SESSION_SCHEMA} file")
    return session


def _write_session(session_file: Path, session: Mapping[str, Any]) -> None:
    _atomic_write_json(session_file, session)


def _read_session_start_condition(session_file: Path) -> dict[str, Any]:
    session = _read_session(session_file)
    condition = session.get("start_condition")
    if condition is None:
        return {
            "schema": START_CONDITION_SCHEMA,
            "type": "immediate",
            "clock": "monotonic_ns",
        }
    if not isinstance(condition, dict):
        raise SoftSyncError(f"{session_file} has invalid start_condition")
    if condition.get("schema") != START_CONDITION_SCHEMA:
        raise SoftSyncError(f"{session_file} start_condition is not {START_CONDITION_SCHEMA}")
    condition_type = condition.get("type")
    if condition_type not in {"immediate", "delay_ns", "timestamp_ns"}:
        raise SoftSyncError(f"{session_file} has unsupported start_condition {condition_type!r}")
    if condition.get("clock", "monotonic_ns") != "monotonic_ns":
        raise SoftSyncError(f"{session_file} start_condition clock must be monotonic_ns")
    return dict(condition)


def _read_session_readiness_gate(session_file: Path) -> dict[str, Any] | None:
    session = _read_session(session_file)
    gate = session.get("readiness_gate")
    if gate is None:
        return None
    if not isinstance(gate, dict):
        raise SoftSyncError(f"{session_file} has invalid readiness_gate")
    if gate.get("schema") != READINESS_GATE_SCHEMA:
        raise SoftSyncError(f"{session_file} readiness_gate is not {READINESS_GATE_SCHEMA}")
    wait_for_clients = gate.get("wait_for_clients")
    wait_for_n_clients = gate.get("wait_for_N_clients")
    if wait_for_clients is None and wait_for_n_clients is None:
        raise SoftSyncError(f"{session_file} readiness_gate has no client requirement")
    if wait_for_clients is not None and wait_for_n_clients is not None:
        raise SoftSyncError(f"{session_file} readiness_gate has conflicting requirements")
    if wait_for_clients is not None:
        if not isinstance(wait_for_clients, list) or not all(
            isinstance(client_id, str) for client_id in wait_for_clients
        ):
            raise SoftSyncError(f"{session_file} readiness_gate wait_for_clients is invalid")
        if not wait_for_clients:
            raise SoftSyncError(
                f"{session_file} readiness_gate wait_for_clients must not be empty"
            )
        if len(set(wait_for_clients)) != len(wait_for_clients):
            raise SoftSyncError(
                f"{session_file} readiness_gate wait_for_clients has duplicates"
            )
    if wait_for_n_clients is not None:
        if not isinstance(wait_for_n_clients, int) or wait_for_n_clients < 0:
            raise SoftSyncError(f"{session_file} readiness_gate wait_for_N_clients is invalid")
    return dict(gate)


def _clear_session_run_config(session_file: Path) -> None:
    session = _read_session(session_file)
    session.pop("start_condition", None)
    session.pop("readiness_gate", None)
    _write_session(session_file, session)


def _unlink_if_exists(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        return
    _fsync_dir(path.parent)


def _deadline(timeout_s: float | None) -> float | None:
    return None if timeout_s is None else time.monotonic() + timeout_s


def _age_ns_from_payload(
    payload: Mapping[str, Any],
    *,
    monotonic_field: str,
    unix_field: str,
    now_monotonic_ns: int | None = None,
    now_unix_ns: int | None = None,
) -> int | None:
    boot_id = payload.get("boot_id")
    at_monotonic_ns = payload.get(monotonic_field)
    if isinstance(at_monotonic_ns, int) and boot_id is not None and boot_id == local_boot_id():
        now = time.monotonic_ns() if now_monotonic_ns is None else now_monotonic_ns
        return max(0, now - at_monotonic_ns)

    at_unix_ns = payload.get(unix_field)
    if isinstance(at_unix_ns, int):
        now = time.time_ns() if now_unix_ns is None else now_unix_ns
        return max(0, now - at_unix_ns)

    return None


def _poll_sleep(deadline: float | None, poll_interval_s: float) -> None:
    if deadline is None:
        time.sleep(poll_interval_s)
        return
    remaining = deadline - time.monotonic()
    if remaining > 0:
        time.sleep(min(poll_interval_s, remaining))


@contextmanager
def _sync_lifecycle_lock(root: Path, sync_id: str) -> Iterator[None]:
    """Serialize host lifecycle transitions for a sync ID."""

    if fcntl is None:
        raise SoftSyncError("host lifecycle locking requires fcntl")

    lock_dir = root / ".locks"
    lock_dir.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_dir / f"{sync_id}.lock", os.O_RDWR | os.O_CREAT, 0o666)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def _clock_now_ns(clock: str) -> int:
    if clock == "monotonic_ns":
        return time.monotonic_ns()
    if clock == "unix_ns":
        return time.time_ns()
    raise ValueError("clock must be 'monotonic_ns' or 'unix_ns'")


def _sleep_until_ns(
    clock: str,
    target_ns: int,
    *,
    cancel_file: Path | None = None,
    stop_file: Path | None = None,
) -> None:
    while True:
        if cancel_file is not None and cancel_file.exists():
            raise SoftSyncCancelled(_read_json(cancel_file))
        if stop_file is not None and stop_file.exists():
            raise SoftSyncStateError(f"{stop_file} already exists")
        remaining_ns = target_ns - _clock_now_ns(clock)
        if remaining_ns <= 0:
            return
        if remaining_ns > _COARSE_SLEEP_SLACK_NS:
            sleep_ns = remaining_ns - _COARSE_SLEEP_SLACK_NS
            time.sleep(min(sleep_ns / 1_000_000_000, _COARSE_SLEEP_MAX_S))
        elif remaining_ns > _FINE_SLEEP_THRESHOLD_NS:
            time.sleep(0.0001)


def _start_payload(
    *,
    sync_id: str,
    coordinator_id: str,
    target_ns: int,
    start_condition: Mapping[str, Any],
    readiness_gate: Mapping[str, Any] | None,
    ready_clients: Sequence[Mapping[str, Any]],
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    issued_at_monotonic_ns = time.monotonic_ns()
    payload: dict[str, Any] = {
        "schema": START_SCHEMA,
        "command": "start",
        "sync_id": sync_id,
        "coordinator_id": coordinator_id,
        "clock": "monotonic_ns",
        "target_ns": target_ns,
        "start_at_monotonic_ns": target_ns,
        "issued_at_monotonic_ns": issued_at_monotonic_ns,
        "issued_at_unix_ns": time.time_ns(),
        "condition": dict(start_condition),
        "ready_clients": [str(client["client_id"]) for client in ready_clients],
        **_identity(),
    }
    if readiness_gate is not None:
        payload["readiness_gate"] = dict(readiness_gate)
    if metadata:
        payload["metadata"] = dict(metadata)
    return payload


def _waiting_for_clients_payload(
    *,
    sync_id: str,
    coordinator_id: str,
    readiness_gate: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema": WAITING_FOR_CLIENTS_SCHEMA,
        "command": "waiting_for_clients",
        "sync_id": sync_id,
        "coordinator_id": coordinator_id,
        "readiness_gate": dict(readiness_gate),
        "issued_at_monotonic_ns": time.monotonic_ns(),
        "issued_at_unix_ns": time.time_ns(),
        **_identity(),
    }


def _normalise_prepare_args(
    *,
    delay_ns: int | None = None,
    timestamp_ns: int | None = None,
    wait_for_N_clients: int | None = None,
    wait_for_clients: Sequence[str] | None = None,
) -> tuple[int | None, int | None, int | None, list[str] | None]:
    if delay_ns is not None and timestamp_ns is not None:
        raise ValueError("provide delay_ns or timestamp_ns, not both")
    if wait_for_clients is not None and wait_for_N_clients is not None:
        raise ValueError("provide wait_for_N_clients or wait_for_clients, not both")

    def non_negative_int(value: int | None, field: str) -> int | None:
        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool):
            raise ValueError(f"{field} must be an integer >= 0")
        if value < 0:
            raise ValueError(f"{field} must be >= 0")
        return value

    delay_ns = non_negative_int(delay_ns, "delay_ns")
    timestamp_ns = non_negative_int(timestamp_ns, "timestamp_ns")
    wait_for_N_clients = non_negative_int(wait_for_N_clients, "wait_for_N_clients")

    client_ids: list[str] | None = None
    if wait_for_clients is not None:
        if isinstance(wait_for_clients, str):
            raise ValueError("wait_for_clients must be a sequence of client_id strings")
        client_ids = [validate_name(client_id, field="client_id") for client_id in wait_for_clients]
        if not client_ids:
            raise ValueError("wait_for_clients must contain at least one client_id")
        if len(set(client_ids)) != len(client_ids):
            raise ValueError("wait_for_clients must not contain duplicate client_ids")

    return delay_ns, timestamp_ns, wait_for_N_clients, client_ids


class _DirectoryWatcher:
    """Persistent directory watcher with a polling fallback."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.fd: int | None = None
        self._open()

    def _open(self) -> None:
        libc_path = ctypes.util.find_library("c")
        if not libc_path or not hasattr(os, "read"):
            return

        try:
            libc = ctypes.CDLL(libc_path, use_errno=True)
            inotify_init1 = libc.inotify_init1
            inotify_add_watch = libc.inotify_add_watch
        except (AttributeError, OSError):
            return

        fd = inotify_init1(_IN_CLOEXEC)
        if fd < 0:
            return

        mask = (
            _IN_CREATE
            | _IN_CLOSE_WRITE
            | _IN_MOVED_TO
            | _IN_ATTRIB
            | _IN_DELETE
            | _IN_Q_OVERFLOW
        )
        watch = inotify_add_watch(fd, os.fsencode(self.path), mask)
        if watch < 0:
            os.close(fd)
            return
        self.fd = fd

    def close(self) -> None:
        if self.fd is None:
            return
        os.close(self.fd)
        self.fd = None

    def wait(self, *, deadline: float | None, fallback_s: float) -> None:
        if self.fd is None:
            _poll_sleep(deadline, fallback_s)
            return

        timeout = None
        if deadline is not None:
            timeout = max(0.0, deadline - time.monotonic())
        ready, _, _ = select.select([self.fd], [], [], timeout)
        if ready:
            try:
                os.read(self.fd, 4096)
            except BlockingIOError:
                pass

    def __enter__(self) -> _DirectoryWatcher:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()


@dataclass(frozen=True)
class StartSignal:
    """Parsed contents of `/tmp/softsync/<sync_id>/start.json`."""

    sync_id: str
    clock: str
    start_at_monotonic_ns: int | None
    start_at_unix_ns: int | None
    issued_at_monotonic_ns: int
    issued_at_unix_ns: int
    coordinator_id: str
    path: Path
    payload: Mapping[str, Any]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any], *, path: Path) -> StartSignal:
        if payload.get("schema") != START_SCHEMA:
            raise SoftSyncError(f"{path} is not a {START_SCHEMA} file")
        clock = payload.get("clock")
        if clock not in {"monotonic_ns", "unix_ns"}:
            raise SoftSyncError(f"{path} has unsupported clock {clock!r}")

        target_ns = payload.get("target_ns")
        start_at_monotonic_ns = payload.get("start_at_monotonic_ns")
        start_at_unix_ns = payload.get("start_at_unix_ns")
        if isinstance(target_ns, int) and clock == "monotonic_ns":
            start_at_monotonic_ns = target_ns
        if isinstance(target_ns, int) and clock == "unix_ns":
            start_at_unix_ns = target_ns
        if clock == "monotonic_ns" and not isinstance(start_at_monotonic_ns, int):
            raise SoftSyncError(f"{path} is missing integer target_ns")
        if clock == "unix_ns" and not isinstance(start_at_unix_ns, int):
            raise SoftSyncError(f"{path} is missing integer target_ns")

        boot_id = payload.get("boot_id")
        current_boot_id = local_boot_id()
        if clock == "monotonic_ns" and boot_id and current_boot_id and boot_id != current_boot_id:
            raise SoftSyncClockMismatch(
                f"{path} was written for boot {boot_id}, local boot is {current_boot_id}"
            )

        return cls(
            sync_id=str(payload["sync_id"]),
            clock=str(clock),
            start_at_monotonic_ns=start_at_monotonic_ns,
            start_at_unix_ns=start_at_unix_ns,
            issued_at_monotonic_ns=int(payload["issued_at_monotonic_ns"]),
            issued_at_unix_ns=int(payload["issued_at_unix_ns"]),
            coordinator_id=str(payload.get("coordinator_id") or ""),
            path=path,
            payload=dict(payload),
        )

    @property
    def start_at_ns(self) -> int:
        if self.clock == "monotonic_ns":
            assert self.start_at_monotonic_ns is not None
            return self.start_at_monotonic_ns
        assert self.start_at_unix_ns is not None
        return self.start_at_unix_ns

    def late_by_ns(self) -> int:
        now_ns = time.monotonic_ns() if self.clock == "monotonic_ns" else time.time_ns()
        return max(0, now_ns - self.start_at_ns)


class Coordinator:
    """Host helper that owns arming and publishing a named start signal."""

    def __init__(
        self,
        sync_id: str,
        *,
        root: str | os.PathLike[str] | None = None,
        coordinator_id: str | None = None,
        reset: bool = False,
        create: bool = True,
    ) -> None:
        self.sync_id = validate_name(sync_id, field="sync_id")
        self.root = Path(root or DEFAULT_ROOT)
        self.path = sync_path(self.sync_id, root=self.root)
        self.coordinator_id = coordinator_id or f"{socket.gethostname()}-{os.getpid()}"
        if reset:
            self.reset()
        elif create:
            with _sync_lifecycle_lock(self.root, self.sync_id):
                self._ensure_session()

    @property
    def session_file(self) -> Path:
        return self.path / "session.json"

    @property
    def start_file(self) -> Path:
        return self.path / "start.json"

    @property
    def waiting_for_clients_file(self) -> Path:
        return self.path / "waiting_for_clients.json"

    @property
    def cancel_file(self) -> Path:
        return self.path / "cancel.json"

    @property
    def stop_file(self) -> Path:
        return self.path / "stop.json"

    @property
    def ready_dir(self) -> Path:
        return self.path / "ready"

    @property
    def active_dir(self) -> Path:
        return self.path / "active"

    @property
    def done_dir(self) -> Path:
        return self.path / "done"

    def _ensure_session(self) -> None:
        self.ready_dir.mkdir(parents=True, exist_ok=True)
        self.active_dir.mkdir(parents=True, exist_ok=True)
        self.done_dir.mkdir(parents=True, exist_ok=True)
        if not self.session_file.exists():
            try:
                _atomic_write_json(
                    self.session_file,
                    {
                        "schema": SESSION_SCHEMA,
                        "sync_id": self.sync_id,
                        "coordinator_id": self.coordinator_id,
                        "created_at_monotonic_ns": time.monotonic_ns(),
                        "created_at_unix_ns": time.time_ns(),
                        **_identity(),
                    },
                    overwrite=False,
                )
            except SoftSyncStateError:
                pass

    def prepare(
        self,
        *,
        delay_ns: int | None = None,
        timestamp_ns: int | None = None,
        wait_for_N_clients: int | None = None,
        wait_for_clients: Sequence[str] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Configure the next start without publishing the go-signal."""

        delay_ns, timestamp_ns, wait_for_N_clients, client_ids = _normalise_prepare_args(
            delay_ns=delay_ns,
            timestamp_ns=timestamp_ns,
            wait_for_N_clients=wait_for_N_clients,
            wait_for_clients=wait_for_clients,
        )

        with _sync_lifecycle_lock(self.root, self.sync_id):
            if (
                self.waiting_for_clients_file.exists()
                or self.start_file.exists()
                or self.stop_file.exists()
                or self.cancel_file.exists()
            ):
                raise SoftSyncStateError(
                    f"{self.path} already has run state; call reset() before prepare()"
                )

            configured_at_monotonic_ns = time.monotonic_ns()
            condition: dict[str, Any] = {
                "schema": START_CONDITION_SCHEMA,
                "clock": "monotonic_ns",
                "configured_at_monotonic_ns": configured_at_monotonic_ns,
                "configured_at_unix_ns": time.time_ns(),
            }
            if delay_ns is not None:
                condition["type"] = "delay_ns"
                condition["delay_ns"] = int(delay_ns)
            elif timestamp_ns is not None:
                condition["type"] = "timestamp_ns"
                condition["timestamp_ns"] = int(timestamp_ns)
            else:
                condition["type"] = "immediate"
            if metadata:
                condition["metadata"] = dict(metadata)

            gate: dict[str, Any] | None = None
            if client_ids is not None:
                gate = {
                    "schema": READINESS_GATE_SCHEMA,
                    "wait_for_clients": sorted(client_ids),
                }
            elif wait_for_N_clients is not None:
                gate = {
                    "schema": READINESS_GATE_SCHEMA,
                    "wait_for_N_clients": int(wait_for_N_clients),
                }

            session = _read_session(self.session_file)
            session["start_condition"] = condition
            if gate is None:
                session.pop("readiness_gate", None)
            else:
                session["readiness_gate"] = gate
            _write_session(self.session_file, session)
            return self.status()

    def reset(self) -> None:
        with _sync_lifecycle_lock(self.root, self.sync_id):
            self.path.mkdir(parents=True, exist_ok=True)
            signal_files = [
                self.waiting_for_clients_file,
                self.start_file,
                self.stop_file,
                self.cancel_file,
            ]
            for path in signal_files:
                _unlink_if_exists(path)
            for directory in [self.ready_dir, self.active_dir, self.done_dir]:
                if directory.exists():
                    shutil.rmtree(directory)
                    _fsync_dir(directory.parent)
            self._ensure_session()
            _clear_session_run_config(self.session_file)

    def _clear_report_files(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        for path in directory.glob("*.json"):
            _unlink_if_exists(path)

    def _cleanup_after_terminal(self) -> None:
        _unlink_if_exists(self.waiting_for_clients_file)
        _unlink_if_exists(self.start_file)
        self._clear_report_files(self.ready_dir)
        self._clear_report_files(self.active_dir)
        _clear_session_run_config(self.session_file)

    def remove(self) -> None:
        with _sync_lifecycle_lock(self.root, self.sync_id):
            if self.path.exists():
                shutil.rmtree(self.path)
                _fsync_dir(self.root)

    def _ready_clients(self) -> list[dict[str, Any]]:
        return [client for client in self.get_clients() if "ready" in client["states"]]

    def _wait_for_readiness_gate(
        self,
        gate: Mapping[str, Any] | None,
        *,
        timestamp_deadline_ns: int | None,
        verbose: bool,
        poll_interval_s: float = 0.01,
    ) -> list[dict[str, Any]]:
        if gate is None:
            return []

        expected_clients = gate.get("wait_for_clients")
        expected = {
            validate_name(client_id, field="client_id")
            for client_id in expected_clients or []
            if isinstance(client_id, str)
        }
        count = gate.get("wait_for_N_clients")
        if count is not None:
            count = int(count)
        if expected and count is not None:
            raise SoftSyncError("readiness gate cannot mix clients and count")
        if not expected and count is None:
            raise SoftSyncError("readiness gate has no requirement")

        while True:
            if self.cancel_file.exists():
                raise SoftSyncCancelled(_read_json(self.cancel_file))
            if self.stop_file.exists():
                raise SoftSyncStateError(f"{self.stop_file} already exists")
            clients = self._ready_clients()
            clients_by_id = {client["client_id"]: client for client in clients}
            if expected and expected.issubset(clients_by_id):
                return [clients_by_id[client_id] for client_id in sorted(expected)]
            if count is not None and len(clients) >= count:
                return clients
            if timestamp_deadline_ns is not None and time.monotonic_ns() > timestamp_deadline_ns:
                raise SoftSyncStateError(
                    "timestamp_ns start condition was missed before clients were ready"
                )
            if verbose:
                missing = sorted(expected.difference(clients_by_id)) if expected else []
                if missing:
                    print(f"waiting for ready clients: {', '.join(missing)}", file=sys.stderr)
                elif count is not None:
                    print(
                        f"waiting for ready clients: {len(clients)}/{count}",
                        file=sys.stderr,
                    )
            _poll_sleep(None, poll_interval_s)

    def start(
        self,
        *,
        verbose: bool = False,
    ) -> StartSignal:
        """Wait for prepared conditions, then publish the active start signal."""

        with _sync_lifecycle_lock(self.root, self.sync_id):
            if self.cancel_file.exists():
                raise SoftSyncStateError(f"{self.cancel_file} already exists")
            if self.stop_file.exists():
                raise SoftSyncStateError(f"{self.stop_file} already exists")
            if self.start_file.exists():
                raise SoftSyncStateError(f"{self.start_file} already exists")
            if self.waiting_for_clients_file.exists():
                raise SoftSyncStateError(f"{self.waiting_for_clients_file} already exists")
            condition = _read_session_start_condition(self.session_file)
            readiness_gate = _read_session_readiness_gate(self.session_file)
            if readiness_gate is not None:
                _atomic_write_json(
                    self.waiting_for_clients_file,
                    _waiting_for_clients_payload(
                        sync_id=self.sync_id,
                        coordinator_id=self.coordinator_id,
                        readiness_gate=readiness_gate,
                    ),
                    overwrite=False,
                )

        deadline_ns = (
            int(condition["timestamp_ns"]) if condition["type"] == "timestamp_ns" else None
        )
        try:
            ready_clients = self._wait_for_readiness_gate(
                readiness_gate,
                timestamp_deadline_ns=deadline_ns,
                verbose=verbose,
            )
        finally:
            if readiness_gate is not None:
                _unlink_if_exists(self.waiting_for_clients_file)

        now_ns = time.monotonic_ns()
        if condition["type"] == "delay_ns":
            target_ns = now_ns + int(condition["delay_ns"])
        elif condition["type"] == "timestamp_ns":
            target_ns = int(condition["timestamp_ns"])
            if now_ns > target_ns:
                raise SoftSyncStateError(
                    "timestamp_ns start condition was missed before clients were ready"
                )
        else:
            target_ns = now_ns

        _sleep_until_ns(
            "monotonic_ns",
            target_ns,
            cancel_file=self.cancel_file,
            stop_file=self.stop_file,
        )

        with _sync_lifecycle_lock(self.root, self.sync_id):
            if self.cancel_file.exists():
                raise SoftSyncStateError(f"{self.cancel_file} already exists")
            if self.stop_file.exists():
                raise SoftSyncStateError(f"{self.stop_file} already exists")
            if self.start_file.exists():
                raise SoftSyncStateError(f"{self.start_file} already exists")
            if self.waiting_for_clients_file.exists():
                raise SoftSyncStateError(f"{self.waiting_for_clients_file} already exists")
            payload = _start_payload(
                sync_id=self.sync_id,
                coordinator_id=self.coordinator_id,
                target_ns=target_ns,
                start_condition=condition,
                readiness_gate=readiness_gate,
                ready_clients=ready_clients,
                metadata=(
                    condition.get("metadata")
                    if isinstance(condition.get("metadata"), dict)
                    else None
                ),
            )
            _atomic_write_json(self.start_file, payload, overwrite=False)
            return StartSignal.from_payload(payload, path=self.start_file)

    def cancel(
        self,
        *,
        reason: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        with _sync_lifecycle_lock(self.root, self.sync_id):
            if self.cancel_file.exists() and not overwrite:
                raise SoftSyncStateError(f"{self.cancel_file} already exists")
            if self.stop_file.exists() and not overwrite:
                raise SoftSyncStateError(f"{self.stop_file} already exists")
            payload: dict[str, Any] = {
                "schema": CANCEL_SCHEMA,
                "command": "cancel",
                "sync_id": self.sync_id,
                "coordinator_id": self.coordinator_id,
                "reason": reason,
                "issued_at_monotonic_ns": time.monotonic_ns(),
                "issued_at_unix_ns": time.time_ns(),
                **_identity(),
            }
            if metadata:
                payload["metadata"] = dict(metadata)
            _atomic_write_json(self.cancel_file, payload, overwrite=overwrite)
            self._cleanup_after_terminal()
            return payload

    def stop(
        self,
        *,
        reason: str | None = None,
        metadata: Mapping[str, Any] | None = None,
        overwrite: bool = False,
    ) -> dict[str, Any]:
        with _sync_lifecycle_lock(self.root, self.sync_id):
            if self.cancel_file.exists() and not overwrite:
                raise SoftSyncStateError(f"{self.cancel_file} already exists")
            if self.stop_file.exists() and not overwrite:
                raise SoftSyncStateError(f"{self.stop_file} already exists")
            payload: dict[str, Any] = {
                "schema": STOP_SCHEMA,
                "command": "stop",
                "sync_id": self.sync_id,
                "coordinator_id": self.coordinator_id,
                "reason": reason,
                "issued_at_monotonic_ns": time.monotonic_ns(),
                "issued_at_unix_ns": time.time_ns(),
                **_identity(),
            }
            if metadata:
                payload["metadata"] = dict(metadata)
            _atomic_write_json(self.stop_file, payload, overwrite=overwrite)
            self._cleanup_after_terminal()
            return payload

    def _read_client_reports(
        self,
        directory: Path,
        *,
        monotonic_field: str,
        unix_field: str,
        now_monotonic_ns: int,
        now_unix_ns: int,
        max_age_ns: int | None,
    ) -> dict[str, dict[str, Any]]:
        reports: dict[str, dict[str, Any]] = {}
        if not directory.exists():
            return reports
        for path in sorted(directory.glob("*.json")):
            try:
                payload = _read_json(path)
            except (OSError, json.JSONDecodeError, SoftSyncError):
                continue
            client_id = payload.get("client_id")
            if not isinstance(client_id, str):
                continue
            age_ns = _age_ns_from_payload(
                payload,
                monotonic_field=monotonic_field,
                unix_field=unix_field,
                now_monotonic_ns=now_monotonic_ns,
                now_unix_ns=now_unix_ns,
            )
            if max_age_ns is not None and (age_ns is None or age_ns > max_age_ns):
                continue
            enriched = dict(payload)
            if age_ns is not None:
                enriched["age_ns"] = age_ns
            reports[client_id] = enriched
        return reports

    def get_clients(self, *, max_age_ns: int | None = None) -> list[dict[str, Any]]:
        """Return clients with recent ready/active/done reports."""

        if max_age_ns is not None and max_age_ns < 0:
            raise ValueError("max_age_ns must be >= 0")
        if not self.path.exists():
            raise SoftSyncNotFound(f"{self.path} does not exist")

        clients: dict[str, dict[str, Any]] = {}
        now_monotonic_ns = time.monotonic_ns()
        now_unix_ns = time.time_ns()
        report_specs = [
            ("ready", self.ready_dir, "ready_at_monotonic_ns", "ready_at_unix_ns"),
            ("active", self.active_dir, "active_at_monotonic_ns", "active_at_unix_ns"),
            ("done", self.done_dir, "completed_at_monotonic_ns", "completed_at_unix_ns"),
        ]
        for state, directory, monotonic_field, unix_field in report_specs:
            reports = self._read_client_reports(
                directory,
                monotonic_field=monotonic_field,
                unix_field=unix_field,
                now_monotonic_ns=now_monotonic_ns,
                now_unix_ns=now_unix_ns,
                max_age_ns=max_age_ns,
            )
            for client_id, report in reports.items():
                client = clients.setdefault(client_id, {"client_id": client_id, "states": []})
                client[state] = report
                client["states"].append(state)
                age_ns = report.get("age_ns")
                if isinstance(age_ns, int):
                    current_latest = client.get("latest_age_ns")
                    if not isinstance(current_latest, int) or age_ns < current_latest:
                        client["latest_age_ns"] = age_ns
        return [clients[name] for name in sorted(clients)]

    def status(self) -> dict[str, Any]:
        if not self.path.exists():
            raise SoftSyncNotFound(f"{self.path} does not exist")
        return {
            "sync_id": self.sync_id,
            "path": str(self.path),
            "start_condition": _read_session_start_condition(self.session_file),
            "readiness_gate": _read_session_readiness_gate(self.session_file),
            "waiting_for_clients": self.waiting_for_clients_file.exists(),
            "started": self.start_file.exists(),
            "stopped": self.stop_file.exists(),
            "cancelled": self.cancel_file.exists(),
            "clients": self.get_clients(),
        }


class Client:
    """Client helper for a process that waits on a named start signal."""

    def __init__(
        self,
        sync_id: str,
        client_id: str | None = None,
        *,
        root: str | os.PathLike[str] | None = None,
    ) -> None:
        self.sync_id = validate_name(sync_id, field="sync_id")
        self.client_id = validate_name(
            client_id or f"{socket.gethostname()}-{os.getpid()}",
            field="client_id",
        )
        self.root = Path(root or DEFAULT_ROOT)
        self.path = sync_path(self.sync_id, root=self.root)

    @property
    def start_file(self) -> Path:
        return self.path / "start.json"

    @property
    def waiting_for_clients_file(self) -> Path:
        return self.path / "waiting_for_clients.json"

    @property
    def cancel_file(self) -> Path:
        return self.path / "cancel.json"

    @property
    def stop_file(self) -> Path:
        return self.path / "stop.json"

    @property
    def ready_file(self) -> Path:
        return self.path / "ready" / f"{self.client_id}.json"

    @property
    def active_file(self) -> Path:
        return self.path / "active" / f"{self.client_id}.json"

    @property
    def done_file(self) -> Path:
        return self.path / "done" / f"{self.client_id}.json"

    def _require_sync_folder(self) -> None:
        if not self.path.is_dir():
            raise SoftSyncNotFound(f"{self.path} does not exist")

    def sync_ready(self, *, metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
        self._require_sync_folder()
        ready_at_monotonic_ns = time.monotonic_ns()
        ready_at_unix_ns = time.time_ns()
        payload: dict[str, Any] = {
            "schema": READY_SCHEMA,
            "sync_id": self.sync_id,
            "client_id": self.client_id,
            "state": "ready",
            "ready_at_monotonic_ns": ready_at_monotonic_ns,
            "ready_at_unix_ns": ready_at_unix_ns,
            **_identity(),
        }
        if metadata:
            payload["metadata"] = dict(metadata)
        _atomic_write_json(self.ready_file, payload, create_parents=False)
        return payload

    def sync_active(self, *, metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
        self._require_sync_folder()
        active_at_monotonic_ns = time.monotonic_ns()
        active_at_unix_ns = time.time_ns()
        payload: dict[str, Any] = {
            "schema": ACTIVE_SCHEMA,
            "sync_id": self.sync_id,
            "client_id": self.client_id,
            "state": "active",
            "active_at_monotonic_ns": active_at_monotonic_ns,
            "active_at_unix_ns": active_at_unix_ns,
            **_identity(),
        }
        if metadata:
            payload["metadata"] = dict(metadata)
        _atomic_write_json(self.active_file, payload, create_parents=False)
        return payload

    def wait_for_ready_request(
        self,
        *,
        timeout_s: float | None = None,
        poll_interval_s: float = 0.005,
    ) -> dict[str, Any]:
        self._require_sync_folder()
        deadline = _deadline(timeout_s)
        with _DirectoryWatcher(self.path) as watcher:
            while True:
                if self.cancel_file.exists():
                    raise SoftSyncCancelled(_read_json(self.cancel_file))
                if self.stop_file.exists():
                    raise SoftSyncCancelled(_read_json(self.stop_file))
                if self.waiting_for_clients_file.exists():
                    try:
                        payload = _read_json(self.waiting_for_clients_file)
                    except FileNotFoundError:
                        payload = None
                    if payload is None:
                        continue
                    if payload.get("schema") != WAITING_FOR_CLIENTS_SCHEMA:
                        raise SoftSyncError(
                            f"{self.waiting_for_clients_file} is not a "
                            f"{WAITING_FOR_CLIENTS_SCHEMA} file"
                        )
                    return payload
                if self.start_file.exists():
                    raise SoftSyncStateError(
                        f"{self.start_file} already exists before readiness was requested"
                    )
                if deadline is not None and time.monotonic() >= deadline:
                    raise SoftSyncTimeout(
                        f"timed out waiting for {self.waiting_for_clients_file}"
                    )
                watcher.wait(deadline=deadline, fallback_s=poll_interval_s)

    def wait_for_start(
        self,
        *,
        timeout_s: float | None = None,
        poll_interval_s: float = 0.005,
    ) -> StartSignal:
        self._require_sync_folder()
        deadline = _deadline(timeout_s)
        with _DirectoryWatcher(self.path) as watcher:
            while True:
                if self.cancel_file.exists():
                    raise SoftSyncCancelled(_read_json(self.cancel_file))
                if self.stop_file.exists():
                    raise SoftSyncCancelled(_read_json(self.stop_file))
                if self.start_file.exists():
                    return StartSignal.from_payload(
                        _read_json(self.start_file),
                        path=self.start_file,
                    )
                if deadline is not None and time.monotonic() >= deadline:
                    raise SoftSyncTimeout(f"timed out waiting for {self.start_file}")
                watcher.wait(deadline=deadline, fallback_s=poll_interval_s)

    def watch_stop(
        self,
        *,
        timeout_s: float | None = None,
        poll_interval_s: float = 0.005,
    ) -> dict[str, Any]:
        self._require_sync_folder()
        deadline = _deadline(timeout_s)
        with _DirectoryWatcher(self.path) as watcher:
            while True:
                if self.cancel_file.exists():
                    raise SoftSyncCancelled(_read_json(self.cancel_file))
                if self.stop_file.exists():
                    return _read_json(self.stop_file)
                if deadline is not None and time.monotonic() >= deadline:
                    raise SoftSyncTimeout(f"timed out waiting for {self.stop_file}")
                watcher.wait(deadline=deadline, fallback_s=poll_interval_s)

    def sync_done(self, *, metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
        self._require_sync_folder()
        payload: dict[str, Any] = {
            "schema": DONE_SCHEMA,
            "sync_id": self.sync_id,
            "client_id": self.client_id,
            "state": "done",
            "completed_at_monotonic_ns": time.monotonic_ns(),
            "completed_at_unix_ns": time.time_ns(),
            **_identity(),
        }
        if metadata:
            payload["metadata"] = dict(metadata)
        _atomic_write_json(self.done_file, payload, create_parents=False)
        return payload


def coordinator(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
    coordinator_id: str | None = None,
    reset: bool = False,
) -> Coordinator:
    return Coordinator(sync_id, root=root, coordinator_id=coordinator_id, reset=reset)


def remove(sync_id: str, *, root: str | os.PathLike[str] | None = None) -> None:
    Coordinator(sync_id, root=root, create=False).remove()


def client(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
) -> Client:
    return Client(sync_id, client_id, root=root)


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
    return Coordinator(sync_id, root=root).start(verbose=verbose)


def stop(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
    reason: str | None = None,
    metadata: Mapping[str, Any] | None = None,
    overwrite: bool = False,
) -> dict[str, Any]:
    return Coordinator(sync_id, root=root).stop(
        reason=reason,
        metadata=metadata,
        overwrite=overwrite,
    )


def wait_for_start(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    poll_interval_s: float = 0.005,
) -> StartSignal:
    return Client(sync_id, client_id, root=root).wait_for_start(
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
    )


def wait_for_ready_request(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    poll_interval_s: float = 0.005,
) -> dict[str, Any]:
    return Client(sync_id, client_id, root=root).wait_for_ready_request(
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
    )


def watch_stop(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    poll_interval_s: float = 0.005,
) -> dict[str, Any]:
    return Client(sync_id, client_id, root=root).watch_stop(
        timeout_s=timeout_s,
        poll_interval_s=poll_interval_s,
    )


def sync_ready(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return Client(sync_id, client_id, root=root).sync_ready(metadata=metadata)


def sync_active(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return Client(sync_id, client_id, root=root).sync_active(metadata=metadata)


def sync_done(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return Client(sync_id, client_id, root=root).sync_done(metadata=metadata)
