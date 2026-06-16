from __future__ import annotations

import json
import runpy
import threading
import time
from pathlib import Path

import pytest

import softsync
from softsync import sync_client, sync_host
from softsync.cli import main


def wait_until(predicate, *, timeout_s: float = 1.0) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if time.monotonic() >= deadline:
            raise AssertionError("condition was not satisfied before timeout")
        time.sleep(0.001)


def assert_uc_payload(payload: dict[str, object], *, kind: str) -> None:
    assert payload["kind"] == kind
    assert isinstance(payload["id"], str)
    assert isinstance(payload["created_at"], str)
    assert str(payload["created_at"]).endswith("Z")
    assert payload["producer"]["project"] == "softsync"  # type: ignore[index]
    assert softsync.SOFTSYNC_UC_PROFILE in payload["contracts"]  # type: ignore[operator]


def test_prepare_immediate_start_and_client_wait_do_not_require_readiness(tmp_path):
    status = sync_host.prepare("experiment-a", root=tmp_path)
    observed: list[softsync.StartSignal] = []

    def wait() -> None:
        observed.append(
            sync_client.wait_for_start(
                "experiment-a",
                root=tmp_path,
                timeout_s=1,
                fallback_poll_interval_s=0.001,
            )
        )

    thread = threading.Thread(target=wait)
    thread.start()
    time.sleep(0.02)

    signal = sync_host.start("experiment-a", root=tmp_path)

    thread.join(timeout=1)

    assert status["start_condition"]["type"] == "immediate"
    assert not thread.is_alive()
    assert observed == [signal]
    assert signal.clock == "monotonic_ns"
    assert signal.start_at_monotonic_ns is not None
    assert (tmp_path / "experiment-a" / "start.json").exists()
    assert list((tmp_path / "experiment-a" / "ready").glob("*.json")) == []


def test_prepare_rejects_conflicting_start_conditions_and_readiness_gates(tmp_path):
    with pytest.raises(ValueError):
        sync_host.prepare(
            "experiment-a",
            root=tmp_path,
            delay_ns=1,
            timestamp_ns=time.monotonic_ns(),
        )
    with pytest.raises(ValueError):
        sync_host.prepare(
            "experiment-a",
            root=tmp_path,
            wait_for_N_clients=1,
            wait_for_clients=["client-a"],
        )


def test_prepare_validates_arguments_before_creating_state(tmp_path):
    invalid_calls = [
        lambda: sync_host.prepare(
            "bad-delay",
            root=tmp_path,
            delay_ns=0.5,  # type: ignore[arg-type]
        ),
        lambda: sync_host.prepare("empty-list", root=tmp_path, wait_for_clients=[]),
        lambda: sync_host.prepare("zero-count", root=tmp_path, wait_for_N_clients=0),
        lambda: sync_host.prepare(
            "duplicate-list",
            root=tmp_path,
            wait_for_clients=["client-a", "client-a"],
        ),
    ]

    for call in invalid_calls:
        with pytest.raises(ValueError):
            call()

    assert list(tmp_path.iterdir()) == []


def test_delay_ns_start_publishes_start_json_after_delay(tmp_path):
    start_path = tmp_path / "experiment-a" / "start.json"
    sync_host.prepare("experiment-a", root=tmp_path, delay_ns=80_000_000)
    observed: list[softsync.StartSignal] = []

    def start() -> None:
        observed.append(sync_host.start("experiment-a", root=tmp_path))

    before = time.monotonic_ns()
    thread = threading.Thread(target=start)
    thread.start()
    time.sleep(0.02)

    assert thread.is_alive()
    assert not start_path.exists()

    thread.join(timeout=1)

    assert not thread.is_alive()
    assert start_path.exists()
    assert observed[0].start_at_monotonic_ns is not None
    assert observed[0].start_at_monotonic_ns >= before + 75_000_000
    assert observed[0].payload["condition"]["delay_ns"] == 80_000_000


def test_timestamp_ns_start_publishes_start_json_at_timestamp(tmp_path):
    start_path = tmp_path / "experiment-a" / "start.json"
    timestamp_ns = time.monotonic_ns() + 80_000_000
    sync_host.prepare("experiment-a", root=tmp_path, timestamp_ns=timestamp_ns)

    signal = sync_host.start("experiment-a", root=tmp_path)

    assert start_path.exists()
    assert signal.start_at_monotonic_ns == timestamp_ns
    assert signal.payload["condition"]["timestamp_ns"] == timestamp_ns


def test_wait_for_start_returns_when_start_json_exists_without_extra_sleep(tmp_path):
    timestamp_ns = time.monotonic_ns() + 50_000_000
    sync_host.prepare("experiment-a", root=tmp_path, timestamp_ns=timestamp_ns)
    sync_host.start("experiment-a", root=tmp_path)
    before_wait = time.monotonic_ns()

    signal = sync_client.wait_for_start("experiment-a", root=tmp_path, timeout_s=0.1)

    assert signal.start_at_monotonic_ns == timestamp_ns
    assert time.monotonic_ns() - before_wait < 20_000_000


def test_wait_for_start_can_begin_before_host_prepare(tmp_path):
    observed: list[softsync.StartSignal] = []

    def wait() -> None:
        observed.append(
            sync_client.wait_for_start(
                "experiment-a",
                root=tmp_path,
                timeout_s=1,
                fallback_poll_interval_s=0.001,
            )
        )

    thread = threading.Thread(target=wait)
    thread.start()
    time.sleep(0.02)

    sync_host.prepare("experiment-a", root=tmp_path)
    signal = sync_host.start("experiment-a", root=tmp_path)

    thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed == [signal]


def test_start_waits_for_n_ready_clients_before_delay(tmp_path):
    sync_host.prepare(
        "experiment-a",
        root=tmp_path,
        delay_ns=40_000_000,
        wait_for_N_clients=2,
    )
    observed: list[softsync.StartSignal] = []

    def start() -> None:
        observed.append(sync_host.start("experiment-a", root=tmp_path))

    thread = threading.Thread(target=start)
    thread.start()
    time.sleep(0.02)

    assert thread.is_alive()
    sync_client.sync_ready("experiment-a", "client-a", root=tmp_path)
    time.sleep(0.02)
    assert thread.is_alive()
    before_second_ready = time.monotonic_ns()
    sync_client.sync_ready("experiment-a", "client-b", root=tmp_path)

    thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed[0].start_at_monotonic_ns is not None
    assert observed[0].start_at_monotonic_ns >= before_second_ready + 35_000_000
    assert observed[0].payload["ready_clients"] == ["client-a", "client-b"]


def test_start_waits_for_named_ready_clients(tmp_path):
    sync_host.prepare(
        "experiment-a",
        root=tmp_path,
        wait_for_clients=["client-b", "client-a"],
    )
    observed: list[softsync.StartSignal] = []

    def start() -> None:
        observed.append(sync_host.start("experiment-a", root=tmp_path))

    thread = threading.Thread(target=start)
    thread.start()
    time.sleep(0.02)

    sync_client.sync_ready("experiment-a", "client-a", root=tmp_path)
    time.sleep(0.02)
    assert thread.is_alive()
    sync_client.sync_ready("experiment-a", "client-b", root=tmp_path)

    thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed[0].payload["readiness_gate"]["wait_for_clients"] == [
        "client-a",
        "client-b",
    ]


def test_host_publishes_waiting_for_clients_marker_until_ready(tmp_path):
    marker_path = tmp_path / "experiment-a" / "waiting_for_clients.json"
    sync_host.prepare("experiment-a", root=tmp_path, wait_for_clients=["client-a"])
    observed: list[softsync.StartSignal] = []

    def start() -> None:
        observed.append(sync_host.start("experiment-a", root=tmp_path))

    thread = threading.Thread(target=start)
    thread.start()
    wait_until(marker_path.exists)

    marker = json.loads(marker_path.read_text(encoding="utf-8"))
    status = sync_host.status("experiment-a", root=tmp_path)

    assert marker["schema"] == softsync.WAITING_FOR_CLIENTS_SCHEMA
    assert marker["command"] == "waiting_for_clients"
    assert marker["readiness_gate"]["wait_for_clients"] == ["client-a"]
    assert status["waiting_for_clients"] is True
    with pytest.raises(softsync.SoftSyncStateError):
        sync_host.prepare("experiment-a", root=tmp_path)

    sync_client.sync_ready("experiment-a", "client-a", root=tmp_path)
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed
    assert not marker_path.exists()
    assert sync_host.status("experiment-a", root=tmp_path)["waiting_for_clients"] is False


def test_client_can_wait_for_ready_request_before_reporting_ready(tmp_path):
    observed: list[softsync.StartSignal] = []
    requests: list[dict[str, object]] = []

    def wait_ready() -> None:
        requests.append(
            sync_client.wait_for_ready_request(
                "experiment-a",
                root=tmp_path,
                timeout_s=1,
                fallback_poll_interval_s=0.001,
            )
        )

    def start() -> None:
        observed.append(sync_host.start("experiment-a", root=tmp_path))

    wait_thread = threading.Thread(target=wait_ready)
    wait_thread.start()
    time.sleep(0.02)

    sync_host.prepare("experiment-a", root=tmp_path, wait_for_clients=["client-a"])
    thread = threading.Thread(target=start)
    thread.start()

    wait_thread.join(timeout=1)
    ready = sync_client.sync_ready("experiment-a", "client-a", root=tmp_path)

    thread.join(timeout=1)

    assert not wait_thread.is_alive()
    assert requests[0]["schema"] == softsync.WAITING_FOR_CLIENTS_SCHEMA
    assert ready["client_id"] == "client-a"
    assert not thread.is_alive()
    assert observed[0].payload["ready_clients"] == ["client-a"]


def test_client_can_discover_ready_sync_ids(tmp_path):
    sync_host.prepare("capture-abc123", root=tmp_path, wait_for_clients=["client-a"])
    sync_host.prepare("other-run", root=tmp_path, wait_for_clients=["client-b"])
    observed: list[softsync.StartSignal] = []

    def start(sync_id: str) -> None:
        observed.append(sync_host.start(sync_id, root=tmp_path))

    threads = [
        threading.Thread(target=start, args=("capture-abc123",)),
        threading.Thread(target=start, args=("other-run",)),
    ]
    for thread in threads:
        thread.start()

    wait_until(
        lambda: sync_client.discover_ready_sync_ids(root=tmp_path)
        == ["capture-abc123", "other-run"],
    )

    discovered = sync_client.discover_ready_sync_ids(root=tmp_path)
    selected_sync_id = next(sync_id for sync_id in discovered if sync_id.startswith("capture-"))

    assert discovered == ["capture-abc123", "other-run"]
    assert selected_sync_id == "capture-abc123"

    sync_client.sync_ready(selected_sync_id, "client-a", root=tmp_path)
    sync_client.sync_ready("other-run", "client-b", root=tmp_path)
    for thread in threads:
        thread.join(timeout=1)

    assert all(not thread.is_alive() for thread in threads)
    assert {signal.sync_id for signal in observed} == {"capture-abc123", "other-run"}


def test_stop_clears_waiting_for_clients_marker(tmp_path):
    marker_path = tmp_path / "experiment-a" / "waiting_for_clients.json"
    errors: list[str] = []
    sync_host.prepare("experiment-a", root=tmp_path, wait_for_clients=["client-a"])

    def start() -> None:
        try:
            sync_host.start("experiment-a", root=tmp_path)
        except softsync.SoftSyncStateError as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=start)
    thread.start()
    wait_until(marker_path.exists)

    sync_host.stop("experiment-a", root=tmp_path, reason="not needed")
    thread.join(timeout=1)

    assert not thread.is_alive()
    assert errors
    assert not marker_path.exists()


def test_timestamp_start_raises_if_clients_are_not_ready_before_timestamp(tmp_path):
    sync_host.prepare(
        "experiment-a",
        root=tmp_path,
        timestamp_ns=time.monotonic_ns() + 30_000_000,
        wait_for_N_clients=1,
    )

    with pytest.raises(softsync.SoftSyncStateError):
        sync_host.start("experiment-a", root=tmp_path)

    assert not (tmp_path / "experiment-a" / "start.json").exists()


def test_sync_ready_active_done_are_visible_to_host(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path)
    ready = sync_client.sync_ready(
        "experiment-a",
        "client-a",
        root=tmp_path,
        metadata={"device": "Muse-1234"},
    )
    active = sync_client.sync_active("experiment-a", "client-a", root=tmp_path)
    done = sync_client.sync_done("experiment-a", "client-a", root=tmp_path)

    clients = sync_host.get_clients("experiment-a", root=tmp_path)

    assert ready["ready_at_unix_ns"] > 0
    assert active["state"] == "active"
    assert done["state"] == "done"
    assert clients[0]["client_id"] == "client-a"
    assert clients[0]["states"] == ["ready", "active", "done"]
    assert clients[0]["ready"]["metadata"]["device"] == "Muse-1234"


def test_runtime_payloads_include_uc_portable_fields(tmp_path):
    status = sync_host.prepare("experiment-a", root=tmp_path, wait_for_clients=["client-a"])
    session = json.loads((tmp_path / "experiment-a" / "session.json").read_text())

    assert_uc_payload(session, kind="softsync_session")
    assert session["lifecycle"]["schema"] == softsync.UC_CORE_LIFECYCLE_CONTRACT
    assert session["lifecycle"]["state"] == "prepared"
    assert status["lifecycle"]["state"] == "prepared"
    assert_uc_payload(status["start_condition"], kind="softsync_start_condition")
    assert_uc_payload(status["readiness_gate"], kind="softsync_readiness_gate")

    observed: list[softsync.StartSignal] = []

    def start() -> None:
        observed.append(sync_host.start("experiment-a", root=tmp_path))

    thread = threading.Thread(target=start)
    thread.start()
    wait_until(lambda: (tmp_path / "experiment-a" / "waiting_for_clients.json").exists())
    waiting = json.loads(
        (tmp_path / "experiment-a" / "waiting_for_clients.json").read_text()
    )

    assert_uc_payload(waiting, kind="softsync_readiness_request")
    assert waiting["lifecycle"]["state"] == "prepared"

    ready = sync_client.sync_ready("experiment-a", "client-a", root=tmp_path)
    thread.join(timeout=1)
    active = sync_client.sync_active("experiment-a", "client-a", root=tmp_path)
    done = sync_client.sync_done("experiment-a", "client-a", root=tmp_path)

    assert not thread.is_alive()
    assert observed
    assert_uc_payload(observed[0].payload, kind="softsync_start_signal")
    assert observed[0].payload["lifecycle"]["state"] == "running"
    assert sync_host.status("experiment-a", root=tmp_path)["lifecycle"]["state"] == "running"
    assert_uc_payload(ready, kind="softsync_client_report")
    assert ready["lifecycle"]["state"] == "prepared"
    assert_uc_payload(active, kind="softsync_client_report")
    assert active["lifecycle"]["state"] == "running"
    assert_uc_payload(done, kind="softsync_client_report")
    assert done["lifecycle"]["state"] == "closed"


def test_sync_active_refreshes_heartbeat_and_supports_stale_filtering(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path)
    first = sync_client.sync_active("experiment-a", "client-a", root=tmp_path)
    stale = dict(first)
    stale.update(
        {
            "active_at_monotonic_ns": time.monotonic_ns() - 10_000_000_000,
            "active_at_unix_ns": time.time_ns() - 10_000_000_000,
        }
    )
    active_path = tmp_path / "experiment-a" / "active" / "client-a.json"
    active_path.write_text(json.dumps(stale), encoding="utf-8")

    stale_clients = sync_host.get_clients(
        "experiment-a",
        root=tmp_path,
        max_age_ns=1_000_000_000,
    )
    all_clients = sync_host.get_clients("experiment-a", root=tmp_path)
    refreshed = sync_client.sync_active("experiment-a", "client-a", root=tmp_path)
    fresh_clients = sync_host.get_clients(
        "experiment-a",
        root=tmp_path,
        max_age_ns=1_000_000_000,
    )

    assert stale_clients == []
    assert all_clients[0]["active"]["age_ns"] >= 1_000_000_000
    assert refreshed["active_at_monotonic_ns"] > first["active_at_monotonic_ns"]
    assert fresh_clients[0]["client_id"] == "client-a"
    assert "active" in fresh_clients[0]["states"]


def test_get_clients_filters_stale_files(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path)
    fresh = sync_client.sync_ready("experiment-a", "fresh", root=tmp_path)
    stale = sync_client.sync_ready("experiment-a", "stale", root=tmp_path)
    stale.update(
        {
            "ready_at_monotonic_ns": time.monotonic_ns() - 10_000_000_000,
            "ready_at_unix_ns": time.time_ns() - 10_000_000_000,
        }
    )
    stale_path = tmp_path / "experiment-a" / "ready" / "stale.json"
    stale_path.write_text(json.dumps(stale), encoding="utf-8")

    all_clients = sync_host.get_clients("experiment-a", root=tmp_path)
    fresh_clients = sync_host.get_clients(
        "experiment-a",
        root=tmp_path,
        max_age_ns=1_000_000_000,
    )

    assert [client["client_id"] for client in all_clients] == ["fresh", "stale"]
    assert [client["client_id"] for client in fresh_clients] == ["fresh"]
    assert fresh["ready_at_unix_ns"] > 0
    assert fresh_clients[0]["ready"]["age_ns"] < 1_000_000_000


def test_stop_is_observed_by_client_and_cleans_active_state(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path)
    sync_host.start("experiment-a", root=tmp_path)
    sync_client.sync_ready("experiment-a", "client-a", root=tmp_path)
    sync_client.sync_active("experiment-a", "client-a", root=tmp_path)
    observed: list[dict[str, object]] = []

    def wait() -> None:
        observed.append(
            sync_client.watch_stop(
                "experiment-a",
                client_id="client-a",
                root=tmp_path,
                timeout_s=1,
            )
        )

    thread = threading.Thread(target=wait)
    thread.start()
    time.sleep(0.02)

    stop = sync_host.stop("experiment-a", root=tmp_path, reason="done")

    thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed == [stop]
    assert not (tmp_path / "experiment-a" / "start.json").exists()
    assert list((tmp_path / "experiment-a" / "ready").glob("*.json")) == []
    assert list((tmp_path / "experiment-a" / "active").glob("*.json")) == []
    assert (tmp_path / "experiment-a" / "stop.json").exists()


def test_ready_active_reports_are_rejected_after_stop_but_done_is_kept(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path)
    sync_host.start("experiment-a", root=tmp_path)
    sync_client.sync_ready("experiment-a", "client-a", root=tmp_path)
    sync_client.sync_active("experiment-a", "client-a", root=tmp_path)

    sync_host.stop("experiment-a", root=tmp_path, reason="done")

    with pytest.raises(softsync.SoftSyncStateError):
        sync_client.sync_ready("experiment-a", "late-client", root=tmp_path)
    with pytest.raises(softsync.SoftSyncStateError):
        sync_client.sync_active("experiment-a", "late-client", root=tmp_path)

    assert sync_host.status("experiment-a", root=tmp_path)["clients"] == []

    sync_client.sync_done("experiment-a", "client-a", root=tmp_path)
    clients = sync_host.get_clients("experiment-a", root=tmp_path)

    assert len(clients) == 1
    assert clients[0]["client_id"] == "client-a"
    assert clients[0]["states"] == ["done"]


def test_cancel_interrupts_client_wait_and_cleans_state(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path, delay_ns=200_000_000)
    observed: list[dict[str, object]] = []

    def wait() -> None:
        try:
            sync_client.wait_for_start("experiment-a", root=tmp_path, timeout_s=1)
        except softsync.SoftSyncCancelled as exc:
            observed.append(exc.payload)

    thread = threading.Thread(target=wait)
    thread.start()
    time.sleep(0.02)

    cancel = sync_host.cancel("experiment-a", root=tmp_path, reason="abort")

    thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed == [cancel]
    assert not (tmp_path / "experiment-a" / "start.json").exists()
    assert (tmp_path / "experiment-a" / "cancel.json").exists()


def test_ready_active_reports_are_rejected_after_cancel(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path)

    sync_host.cancel("experiment-a", root=tmp_path, reason="abort")

    with pytest.raises(softsync.SoftSyncStateError):
        sync_client.sync_ready("experiment-a", "late-client", root=tmp_path)
    with pytest.raises(softsync.SoftSyncStateError):
        sync_client.sync_active("experiment-a", "late-client", root=tmp_path)

    assert sync_host.status("experiment-a", root=tmp_path)["clients"] == []


def test_stop_interrupts_delayed_host_start_before_signal(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path, delay_ns=500_000_000)
    errors: list[str] = []

    def start() -> None:
        try:
            sync_host.start("experiment-a", root=tmp_path)
        except softsync.SoftSyncStateError as exc:
            errors.append(str(exc))

    thread = threading.Thread(target=start)
    thread.start()
    time.sleep(0.02)

    sync_host.stop("experiment-a", root=tmp_path, reason="not needed")

    thread.join(timeout=0.3)

    assert not thread.is_alive()
    assert errors
    assert not (tmp_path / "experiment-a" / "start.json").exists()
    assert (tmp_path / "experiment-a" / "stop.json").exists()


def test_reset_clears_state_but_keeps_sync_folder(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path, delay_ns=1)
    sync_host.start("experiment-a", root=tmp_path)
    sync_client.sync_ready("experiment-a", "client-a", root=tmp_path)
    sync_host.stop("experiment-a", root=tmp_path)

    sync_host.reset("experiment-a", root=tmp_path)

    status = sync_host.status("experiment-a", root=tmp_path)
    assert (tmp_path / "experiment-a").exists()
    assert (tmp_path / "experiment-a" / "session.json").exists()
    assert not (tmp_path / "experiment-a" / "stop.json").exists()
    assert status["start_condition"]["type"] == "immediate"
    assert status["clients"] == []


def test_remove_deletes_sync_folder(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path)

    sync_host.remove("experiment-a", root=tmp_path)

    assert not (tmp_path / "experiment-a").exists()


def test_read_only_missing_sync_raises_without_creating_state(tmp_path):
    with pytest.raises(softsync.SoftSyncNotFound):
        sync_host.get_clients("missing", root=tmp_path)
    with pytest.raises(softsync.SoftSyncNotFound):
        sync_host.status("missing", root=tmp_path)

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "call",
    [
        lambda root: sync_client.wait_for_start("missing", root=root, timeout_s=0.01),
        lambda root: sync_client.wait_for_ready_request("missing", root=root, timeout_s=0.01),
        lambda root: sync_client.watch_stop("missing", root=root, timeout_s=0.01),
    ],
)
def test_client_wait_methods_missing_sync_timeout_without_creating_state(tmp_path, call):
    with pytest.raises(softsync.SoftSyncTimeout):
        call(tmp_path)

    assert list(tmp_path.iterdir()) == []


@pytest.mark.parametrize(
    "call",
    [
        lambda root: sync_client.sync_ready("missing", "client-a", root=root),
        lambda root: sync_client.sync_active("missing", "client-a", root=root),
        lambda root: sync_client.sync_done("missing", "client-a", root=root),
    ],
)
def test_client_report_methods_missing_sync_raise_without_creating_state(tmp_path, call):
    with pytest.raises(softsync.SoftSyncNotFound):
        call(tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_sync_ids_are_path_safe(tmp_path):
    with pytest.raises(ValueError):
        sync_host.prepare("../experiment-a", root=tmp_path)


def test_top_level_convenience_exports_match_role_modules():
    assert softsync.cancel is sync_host.cancel
    assert softsync.reset is sync_host.reset
    assert softsync.status is sync_host.status
    assert softsync.validate is sync_host.validate
    assert softsync.discover_ready_sync_ids is sync_client.discover_ready_sync_ids
    assert softsync.wait_for_ready_request is sync_client.wait_for_ready_request
    assert softsync.wait_for_start is sync_client.wait_for_start


def test_existing_start_requires_reset(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path)
    sync_host.start("experiment-a", root=tmp_path)

    with pytest.raises(softsync.SoftSyncStateError):
        sync_host.start("experiment-a", root=tmp_path)
    with pytest.raises(softsync.SoftSyncStateError):
        sync_host.prepare("experiment-a", root=tmp_path)


def test_concurrent_start_has_one_winner(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path)
    barrier = threading.Barrier(3)
    lock = threading.Lock()
    results: list[str] = []

    def start() -> None:
        barrier.wait(timeout=1)
        try:
            sync_host.start("experiment-a", root=tmp_path)
        except softsync.SoftSyncStateError:
            result = "state-error"
        else:
            result = "started"
        with lock:
            results.append(result)

    threads = [threading.Thread(target=start), threading.Thread(target=start)]
    for thread in threads:
        thread.start()

    barrier.wait(timeout=1)
    for thread in threads:
        thread.join(timeout=1)

    assert all(not thread.is_alive() for thread in threads)
    assert sorted(results) == ["started", "state-error"]
    assert (tmp_path / "experiment-a" / "start.json").exists()


def test_cli_reports_expected_errors_without_traceback(tmp_path, capsys):
    exit_code = main(["--root", str(tmp_path), "prepare", "../bad"])

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "sync_id must match" in captured.err
    assert "Traceback" not in captured.err
    assert captured.out == ""


def test_cli_prepare_start_wait_flow(tmp_path, capsys):
    assert main(["--root", str(tmp_path), "prepare", "experiment-a", "--delay-ns", "1"]) == 0
    assert main(["--root", str(tmp_path), "start", "experiment-a"]) == 0
    assert main(["--root", str(tmp_path), "wait", "experiment-a"]) == 0

    captured = capsys.readouterr()

    assert '"command": "start"' in captured.out


def test_cli_wait_ready_flow(tmp_path, capsys):
    assert (
        main(
            [
                "--root",
                str(tmp_path),
                "prepare",
                "experiment-a",
                "--wait-for-client",
                "client-a",
            ]
        )
        == 0
    )
    observed: list[softsync.StartSignal] = []

    def start() -> None:
        observed.append(sync_host.start("experiment-a", root=tmp_path))

    thread = threading.Thread(target=start)
    thread.start()

    assert (
        main(
            [
                "--root",
                str(tmp_path),
                "discover-ready",
            ]
        )
        == 0
    )
    assert (
        main(
            [
                "--root",
                str(tmp_path),
                "wait-ready",
                "experiment-a",
                "--timeout",
                "1",
            ]
        )
        == 0
    )
    assert main(["--root", str(tmp_path), "ready", "experiment-a", "client-a"]) == 0

    thread.join(timeout=1)
    captured = capsys.readouterr()

    assert not thread.is_alive()
    assert observed
    assert '"command": "waiting_for_clients"' in captured.out
    assert '"sync_id": "experiment-a"' in captured.out


def test_validate_packaged_uc_descriptors(capsys):
    assert main(["validate"]) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload["schema"] == softsync.VALIDATION_SCHEMA
    assert payload["results"]["status"] == "passed"
    assert payload["conformance"]["profile"] == softsync.SOFTSYNC_UC_PROFILE


def test_validate_sync_folder_and_detects_missing_uc_fields(tmp_path):
    sync_host.prepare("experiment-a", root=tmp_path)
    sync_host.start("experiment-a", root=tmp_path)

    valid = sync_host.validate("experiment-a", root=tmp_path)

    assert valid["results"]["status"] == "passed"
    assert valid["conformance"]["levels"] == ["shape", "portable"]

    start_path = tmp_path / "experiment-a" / "start.json"
    tampered = json.loads(start_path.read_text(encoding="utf-8"))
    tampered.pop("id")
    start_path.write_text(json.dumps(tampered), encoding="utf-8")

    invalid = sync_host.validate("experiment-a", root=tmp_path)

    assert invalid["results"]["status"] == "failed"
    assert any("missing UC field 'id'" in error for error in invalid["errors"])


def test_cli_validate_sync_folder(tmp_path, capsys):
    sync_host.prepare("experiment-a", root=tmp_path)

    assert main(["--root", str(tmp_path), "validate", "experiment-a"]) == 0

    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload["schema"] == softsync.VALIDATION_SCHEMA
    assert payload["results"]["status"] == "passed"


def test_client_example_can_start_before_host(tmp_path, monkeypatch):
    import softsync.core as core

    monkeypatch.setattr(core, "DEFAULT_ROOT", tmp_path)
    example_path = Path(__file__).resolve().parents[1] / "examples" / "client.py"
    client_errors: list[BaseException] = []
    start_errors: list[BaseException] = []

    def run_client() -> None:
        try:
            runpy.run_path(str(example_path), run_name="__main__")
        except BaseException as exc:
            client_errors.append(exc)

    def run_start() -> None:
        try:
            sync_host.start("softsync-example", root=tmp_path)
        except BaseException as exc:
            start_errors.append(exc)

    client_thread = threading.Thread(target=run_client)
    client_thread.start()

    sync_host.prepare(
        "softsync-example",
        root=tmp_path,
        reset=True,
        wait_for_clients=["client-01"],
    )
    start_thread = threading.Thread(target=run_start)
    start_thread.start()
    start_thread.join(timeout=2)
    if start_thread.is_alive():
        sync_host.cancel("softsync-example", root=tmp_path)
    start_thread.join(timeout=1)

    assert not start_thread.is_alive()
    assert start_errors == []

    sync_host.stop("softsync-example", root=tmp_path, reason="example complete")
    client_thread.join(timeout=2)

    assert not client_thread.is_alive()
    assert client_errors == []
    assert sync_host.get_clients("softsync-example", root=tmp_path)[0]["states"] == ["done"]
