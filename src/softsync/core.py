from __future__ import annotations

import json
import os
import re
import select
import shutil
import socket
import subprocess
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
PENDING_START_SCHEMA = "softsync.pending_start.v1"
SCHEDULED_START_SCHEMA = "softsync.scheduled_start.v1"
STOP_SCHEMA = "softsync.stop.v1"
CANCEL_SCHEMA = "softsync.cancel.v1"
READY_SCHEMA = "softsync.ready.v1"
ACTIVE_SCHEMA = "softsync.active.v1"
CLIENT_STARTED_SCHEMA = "softsync.client_started.v1"
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
    """Raised when a client observes a cancel flag before a start flag."""

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


def _read_session_scheduled_start(session_file: Path) -> dict[str, Any] | None:
    session = _read_json(session_file)
    scheduled = session.get("scheduled_start")
    if scheduled is None:
        return None
    if not isinstance(scheduled, dict):
        raise SoftSyncError(f"{session_file} has invalid scheduled_start")
    if scheduled.get("schema") != SCHEDULED_START_SCHEMA:
        raise SoftSyncError(f"{session_file} scheduled_start is not {SCHEDULED_START_SCHEMA}")
    return dict(scheduled)


def _write_session_scheduled_start(
    session_file: Path,
    schedule: Mapping[str, Any] | None,
) -> None:
    session = _read_json(session_file)
    if schedule is None:
        session.pop("scheduled_start", None)
    else:
        session["scheduled_start"] = dict(schedule)
    _atomic_write_json(session_file, session)


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


def _sleep_until_ns(clock: str, target_ns: int, *, cancel_file: Path | None = None) -> None:
    while True:
        if cancel_file is not None and cancel_file.exists():
            raise SoftSyncCancelled(_read_json(cancel_file))
        remaining_ns = target_ns - _clock_now_ns(clock)
        if remaining_ns <= 0:
            return
        if remaining_ns > _COARSE_SLEEP_SLACK_NS:
            sleep_ns = remaining_ns - _COARSE_SLEEP_SLACK_NS
            time.sleep(min(sleep_ns / 1_000_000_000, _COARSE_SLEEP_MAX_S))
        elif remaining_ns > _FINE_SLEEP_THRESHOLD_NS:
            time.sleep(0.0001)


def _start_payload_from_schedule(schedule: Mapping[str, Any]) -> dict[str, Any]:
    clock = str(schedule["clock"])
    target_ns = int(schedule["target_ns"])
    issued_at_monotonic_ns = time.monotonic_ns()
    issued_at_unix_ns = time.time_ns()
    payload: dict[str, Any] = {
        "schema": START_SCHEMA,
        "command": "start",
        "sync_id": str(schedule["sync_id"]),
        "coordinator_id": str(schedule.get("coordinator_id") or ""),
        "clock": clock,
        "target_ns": target_ns,
        "issued_at_monotonic_ns": issued_at_monotonic_ns,
        "issued_at_unix_ns": issued_at_unix_ns,
        "armed": bool(schedule.get("armed")),
        **_identity(),
    }
    if clock == "monotonic_ns":
        payload["start_at_monotonic_ns"] = target_ns
    else:
        payload["start_at_unix_ns"] = target_ns

    passthrough_fields = [
        "delay_s",
        "delay_ns",
        "configured_target_ns",
        "configured_at_monotonic_ns",
        "configured_at_unix_ns",
        "metadata",
        "scheduled_at_monotonic_ns",
        "scheduled_at_unix_ns",
    ]
    for field in passthrough_fields:
        if field in schedule:
            payload[field] = schedule[field]
    return payload


def _publish_scheduled_start(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
) -> StartSignal:
    sync_id = validate_name(sync_id, field="sync_id")
    root_path = Path(root or DEFAULT_ROOT)
    sync_dir = sync_path(sync_id, root=root_path)
    session_file = sync_dir / "session.json"
    start_file = sync_dir / "start.json"
    pending_start_file = sync_dir / "pending_start.json"
    cancel_file = sync_dir / "cancel.json"

    while True:
        if cancel_file.exists():
            raise SoftSyncCancelled(_read_json(cancel_file))
        schedule = _read_session_scheduled_start(session_file)
        if schedule is None:
            if start_file.exists():
                return StartSignal.from_payload(_read_json(start_file), path=start_file)
            raise SoftSyncNotFound(f"{session_file} has no scheduled_start")
        clock = str(schedule["clock"])
        target_ns = int(schedule["target_ns"])
        remaining_ns = target_ns - _clock_now_ns(clock)
        if remaining_ns <= 0:
            break
        time.sleep(min(remaining_ns / 1_000_000_000, _COARSE_SLEEP_MAX_S))

    with _sync_lifecycle_lock(root_path, sync_id):
        if cancel_file.exists():
            cancel_payload = _read_json(cancel_file)
            _write_session_scheduled_start(session_file, None)
            raise SoftSyncCancelled(cancel_payload)
        schedule = _read_session_scheduled_start(session_file)
        if schedule is None:
            if start_file.exists():
                return StartSignal.from_payload(_read_json(start_file), path=start_file)
            raise SoftSyncNotFound(f"{session_file} has no scheduled_start")
        overwrite = bool(schedule.get("overwrite"))
        if start_file.exists() and not overwrite:
            raise SoftSyncStateError(f"{start_file} already exists")
        payload = _start_payload_from_schedule(schedule)
        _atomic_write_json(start_file, payload, overwrite=overwrite)
        _unlink_if_exists(pending_start_file)
        _write_session_scheduled_start(session_file, None)
        return StartSignal.from_payload(payload, path=start_file)


def _spawn_scheduled_start_publisher(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
) -> None:
    env = os.environ.copy()
    src_root = str(Path(__file__).resolve().parents[1])
    current_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        src_root if not current_pythonpath else f"{src_root}{os.pathsep}{current_pythonpath}"
    )
    subprocess.Popen(
        [
            sys.executable,
            "-m",
            "softsync._start_publisher",
            sync_id,
            str(Path(root or DEFAULT_ROOT)),
        ],
        close_fds=True,
        env=env,
        start_new_session=True,
        stderr=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
    )


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
class PendingStart:
    """Parsed contents of `/tmp/softsync/<sync_id>/pending_start.json`."""

    sync_id: str
    clock: str
    delay_s: float | None
    target_ns: int | None
    configured_at_monotonic_ns: int
    configured_at_unix_ns: int
    coordinator_id: str
    path: Path
    payload: Mapping[str, Any]

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any], *, path: Path) -> PendingStart:
        if payload.get("schema") != PENDING_START_SCHEMA:
            raise SoftSyncError(f"{path} is not a {PENDING_START_SCHEMA} file")
        clock = payload.get("clock")
        if clock not in {"monotonic_ns", "unix_ns"}:
            raise SoftSyncError(f"{path} has unsupported clock {clock!r}")

        delay_s = payload.get("delay_s")
        target_ns = payload.get("target_ns")
        if delay_s is None and target_ns is None:
            raise SoftSyncError(f"{path} must contain delay_s or target_ns")
        if delay_s is not None and target_ns is not None:
            raise SoftSyncError(f"{path} must not contain both delay_s and target_ns")
        if delay_s is not None:
            delay_s = float(delay_s)
            if delay_s < 0:
                raise SoftSyncError(f"{path} has negative delay_s")
        if target_ns is not None and not isinstance(target_ns, int):
            raise SoftSyncError(f"{path} has non-integer target_ns")

        return cls(
            sync_id=str(payload["sync_id"]),
            clock=str(clock),
            delay_s=delay_s,
            target_ns=target_ns,
            configured_at_monotonic_ns=int(payload["configured_at_monotonic_ns"]),
            configured_at_unix_ns=int(payload["configured_at_unix_ns"]),
            coordinator_id=str(payload.get("coordinator_id") or ""),
            path=path,
            payload=dict(payload),
        )

    def resolve_target_ns(
        self,
        *,
        issued_at_monotonic_ns: int,
        issued_at_unix_ns: int,
    ) -> int:
        if self.target_ns is not None:
            return self.target_ns
        assert self.delay_s is not None
        delay_ns = int(self.delay_s * 1_000_000_000)
        if self.clock == "monotonic_ns":
            return issued_at_monotonic_ns + delay_ns
        return issued_at_unix_ns + delay_ns


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

    def kwargs(self) -> dict[str, int]:
        """Return start timestamp kwargs for prepare-start APIs."""

        if self.clock == "monotonic_ns":
            assert self.start_at_monotonic_ns is not None
            return {"start_at_monotonic_ns": self.start_at_monotonic_ns}
        assert self.start_at_unix_ns is not None
        return {"start_at_unix_ns": self.start_at_unix_ns}

    def sleep_until_start(self) -> None:
        while True:
            now_ns = time.monotonic_ns() if self.clock == "monotonic_ns" else time.time_ns()
            remaining_ns = self.start_at_ns - now_ns
            if remaining_ns <= 0:
                return
            if remaining_ns > _COARSE_SLEEP_SLACK_NS:
                sleep_ns = remaining_ns - _COARSE_SLEEP_SLACK_NS
                time.sleep(min(sleep_ns / 1_000_000_000, _COARSE_SLEEP_MAX_S))
            elif remaining_ns > _FINE_SLEEP_THRESHOLD_NS:
                time.sleep(0.0001)

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
    def pending_start_file(self) -> Path:
        return self.path / "pending_start.json"

    @property
    def session_file(self) -> Path:
        return self.path / "session.json"

    @property
    def start_file(self) -> Path:
        return self.path / "start.json"

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
    def started_dir(self) -> Path:
        return self.path / "started"

    @property
    def done_dir(self) -> Path:
        return self.path / "done"

    def _ensure_session(self) -> None:
        self.ready_dir.mkdir(parents=True, exist_ok=True)
        self.active_dir.mkdir(parents=True, exist_ok=True)
        self.started_dir.mkdir(parents=True, exist_ok=True)
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

    def reset(self) -> None:
        with _sync_lifecycle_lock(self.root, self.sync_id):
            self.path.mkdir(parents=True, exist_ok=True)
            signal_files = [
                self.pending_start_file,
                self.start_file,
                self.stop_file,
                self.cancel_file,
            ]
            for path in signal_files:
                _unlink_if_exists(path)
            for directory in [self.ready_dir, self.active_dir, self.started_dir, self.done_dir]:
                if directory.exists():
                    shutil.rmtree(directory)
                    _fsync_dir(directory.parent)
            self._ensure_session()
            _write_session_scheduled_start(self.session_file, None)

    def remove(self) -> None:
        with _sync_lifecycle_lock(self.root, self.sync_id):
            if self.path.exists():
                shutil.rmtree(self.path)
                _fsync_dir(self.root)

    def set_start(
        self,
        *,
        delay_s: float | None = None,
        clock: str = "monotonic_ns",
        target_ns: int | None = None,
        metadata: Mapping[str, Any] | None = None,
        overwrite: bool = False,
    ) -> PendingStart:
        """Arm a future start boundary without publishing the active start signal."""

        with _sync_lifecycle_lock(self.root, self.sync_id):
            if delay_s is None and target_ns is None:
                raise ValueError("provide delay_s or target_ns")
            if delay_s is not None and target_ns is not None:
                raise ValueError("provide delay_s or target_ns, not both")
            if delay_s is not None and delay_s < 0:
                raise ValueError("delay_s must be >= 0")
            if clock not in {"monotonic_ns", "unix_ns"}:
                raise ValueError("clock must be 'monotonic_ns' or 'unix_ns'")
            if self.cancel_file.exists() and not overwrite:
                raise SoftSyncStateError(f"{self.cancel_file} already exists")
            if self.start_file.exists() and not overwrite:
                raise SoftSyncStateError(f"{self.start_file} already exists")
            scheduled_start = _read_session_scheduled_start(self.session_file)
            if scheduled_start is not None and not overwrite:
                raise SoftSyncStateError(f"{self.session_file} already has scheduled_start")
            if self.pending_start_file.exists() and not overwrite:
                raise SoftSyncStateError(f"{self.pending_start_file} already exists")

            payload: dict[str, Any] = {
                "schema": PENDING_START_SCHEMA,
                "command": "set_start",
                "sync_id": self.sync_id,
                "coordinator_id": self.coordinator_id,
                "clock": clock,
                "configured_at_monotonic_ns": time.monotonic_ns(),
                "configured_at_unix_ns": time.time_ns(),
                **_identity(),
            }
            if delay_s is not None:
                payload["delay_s"] = float(delay_s)
                payload["delay_ns"] = int(delay_s * 1_000_000_000)
            else:
                assert target_ns is not None
                payload["target_ns"] = int(target_ns)
            if metadata:
                payload["metadata"] = dict(metadata)

            _atomic_write_json(self.pending_start_file, payload, overwrite=overwrite)
            return PendingStart.from_payload(payload, path=self.pending_start_file)

    def start(
        self,
        *,
        clock: str = "monotonic_ns",
        metadata: Mapping[str, Any] | None = None,
        overwrite: bool = False,
        nonblocking: bool = False,
    ) -> StartSignal:
        """Publish the active start signal at its target boundary."""

        with _sync_lifecycle_lock(self.root, self.sync_id):
            if clock not in {"monotonic_ns", "unix_ns"}:
                raise ValueError("clock must be 'monotonic_ns' or 'unix_ns'")
            if self.cancel_file.exists() and not overwrite:
                raise SoftSyncStateError(f"{self.cancel_file} already exists")
            if self.start_file.exists() and not overwrite:
                raise SoftSyncStateError(f"{self.start_file} already exists")
            scheduled_start = _read_session_scheduled_start(self.session_file)
            if scheduled_start is not None and not overwrite:
                raise SoftSyncStateError(f"{self.session_file} already has scheduled_start")

            pending = (
                PendingStart.from_payload(
                    _read_json(self.pending_start_file),
                    path=self.pending_start_file,
                )
                if self.pending_start_file.exists()
                else None
            )
            scheduled_at_monotonic_ns = time.monotonic_ns()
            scheduled_at_unix_ns = time.time_ns()
            resolved_clock = pending.clock if pending else clock
            target_ns = (
                pending.resolve_target_ns(
                    issued_at_monotonic_ns=scheduled_at_monotonic_ns,
                    issued_at_unix_ns=scheduled_at_unix_ns,
                )
                if pending
                else (
                    scheduled_at_monotonic_ns
                    if resolved_clock == "monotonic_ns"
                    else scheduled_at_unix_ns
                )
            )

            schedule: dict[str, Any] = {
                "schema": SCHEDULED_START_SCHEMA,
                "command": "schedule_start",
                "sync_id": self.sync_id,
                "coordinator_id": self.coordinator_id,
                "clock": resolved_clock,
                "target_ns": int(target_ns),
                "scheduled_at_monotonic_ns": scheduled_at_monotonic_ns,
                "scheduled_at_unix_ns": scheduled_at_unix_ns,
                "armed": pending is not None,
                "overwrite": overwrite,
                **_identity(),
            }
            if pending:
                if pending.delay_s is not None:
                    schedule["delay_s"] = pending.delay_s
                    schedule["delay_ns"] = int(pending.delay_s * 1_000_000_000)
                else:
                    schedule["configured_target_ns"] = pending.target_ns
                if pending.payload.get("metadata"):
                    schedule["metadata"] = dict(pending.payload["metadata"])
                schedule["configured_at_monotonic_ns"] = pending.configured_at_monotonic_ns
                schedule["configured_at_unix_ns"] = pending.configured_at_unix_ns
            if metadata:
                merged_metadata = dict(schedule.get("metadata") or {})
                merged_metadata.update(dict(metadata))
                schedule["metadata"] = merged_metadata

            target_is_due = target_ns <= _clock_now_ns(resolved_clock)
            if target_is_due:
                payload = _start_payload_from_schedule(schedule)
                _atomic_write_json(self.start_file, payload, overwrite=overwrite)
                _unlink_if_exists(self.pending_start_file)
                return StartSignal.from_payload(payload, path=self.start_file)

            _write_session_scheduled_start(self.session_file, schedule)
            _unlink_if_exists(self.pending_start_file)
            predicted_payload = _start_payload_from_schedule(schedule)

        if nonblocking:
            _spawn_scheduled_start_publisher(self.sync_id, root=self.root)
            return StartSignal.from_payload(predicted_payload, path=self.start_file)
        return _publish_scheduled_start(self.sync_id, root=self.root)

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
        """Return clients with recent ready/active/started/done reports."""

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
            ("started", self.started_dir, "started_at_monotonic_ns", "started_at_unix_ns"),
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

    def wait_for_clients(
        self,
        client_ids: Sequence[str] | None = None,
        *,
        count: int | None = None,
        max_age_ns: int | None = None,
        timeout_s: float | None = None,
        poll_interval_s: float = 0.01,
    ) -> list[dict[str, Any]]:
        """Optional barrier: wait for named clients or a minimum ready count."""

        if client_ids is None and count is None:
            raise ValueError("provide client_ids or count")
        expected = {validate_name(client_id, field="client_id") for client_id in client_ids or ()}
        deadline = _deadline(timeout_s)
        while True:
            clients = [
                client
                for client in self.get_clients(max_age_ns=max_age_ns)
                if "ready" in client["states"]
            ]
            clients_by_id = {client["client_id"]: client for client in clients}
            if expected and expected.issubset(clients_by_id):
                return [clients_by_id[client_id] for client_id in sorted(expected)]
            if count is not None and len(clients) >= count:
                return clients
            if deadline is not None and time.monotonic() >= deadline:
                missing = sorted(expected.difference(clients_by_id)) if expected else []
                detail = f": {', '.join(missing)}" if missing else ""
                raise SoftSyncTimeout(f"timed out waiting for ready clients{detail}")
            _poll_sleep(deadline, poll_interval_s)

    def status(self) -> dict[str, Any]:
        if not self.path.exists():
            raise SoftSyncNotFound(f"{self.path} does not exist")
        return {
            "sync_id": self.sync_id,
            "path": str(self.path),
            "armed": self.pending_start_file.exists(),
            "started": self.start_file.exists(),
            "scheduled": _read_session_scheduled_start(self.session_file) is not None,
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
    def started_file(self) -> Path:
        return self.path / "started" / f"{self.client_id}.json"

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

    def watch_start(
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

    def sync_watch(
        self,
        *,
        timeout_s: float | None = None,
        poll_interval_s: float = 0.005,
    ) -> StartSignal:
        signal = self.watch_start(timeout_s=timeout_s, poll_interval_s=poll_interval_s)
        signal.sleep_until_start()
        return signal

    def report_start(self, *, metadata: Mapping[str, Any] | None = None) -> dict[str, Any]:
        self._require_sync_folder()
        started_at_monotonic_ns = time.monotonic_ns()
        started_at_unix_ns = time.time_ns()
        payload: dict[str, Any] = {
            "schema": CLIENT_STARTED_SCHEMA,
            "sync_id": self.sync_id,
            "client_id": self.client_id,
            "state": "started",
            "started_at_monotonic_ns": started_at_monotonic_ns,
            "started_at_unix_ns": started_at_unix_ns,
            **_identity(),
        }
        if metadata:
            payload["metadata"] = dict(metadata)
        _atomic_write_json(self.started_file, payload, create_parents=False)
        return payload

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


def set_start(
    sync_id: str,
    *,
    root: str | os.PathLike[str] | None = None,
    delay_s: float | None = None,
    clock: str = "monotonic_ns",
    target_ns: int | None = None,
    metadata: Mapping[str, Any] | None = None,
    overwrite: bool = False,
    reset: bool = False,
) -> PendingStart:
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
    root: str | os.PathLike[str] | None = None,
    clock: str = "monotonic_ns",
    metadata: Mapping[str, Any] | None = None,
    overwrite: bool = False,
    reset: bool = False,
    nonblocking: bool = False,
) -> StartSignal:
    return Coordinator(sync_id, root=root, reset=reset).start(
        clock=clock,
        metadata=metadata,
        overwrite=overwrite,
        nonblocking=nonblocking,
    )


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


def watch_start(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    poll_interval_s: float = 0.005,
) -> StartSignal:
    return Client(sync_id, client_id, root=root).watch_start(
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


def sync_watch(
    sync_id: str,
    *,
    client_id: str | None = None,
    root: str | os.PathLike[str] | None = None,
    timeout_s: float | None = None,
    poll_interval_s: float = 0.005,
) -> StartSignal:
    return Client(sync_id, client_id, root=root).sync_watch(
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


def report_start(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return Client(sync_id, client_id, root=root).report_start(metadata=metadata)


def sync_done(
    sync_id: str,
    client_id: str | None = None,
    *,
    root: str | os.PathLike[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return Client(sync_id, client_id, root=root).sync_done(metadata=metadata)
