from __future__ import annotations

import json
import threading
import time

import pytest

import soft_sync
from soft_sync.cli import main
from soft_sync import sync_client, sync_host


def test_host_start_and_client_sync_watch_do_not_require_readiness(tmp_path):
    observed: list[soft_sync.StartSignal] = []

    def wait() -> None:
        observed.append(
            sync_client.sync_watch(
                "experiment-a",
                root=tmp_path,
                timeout_s=1,
                fallback_poll_interval_s=0.001,
            )
        )

    thread = threading.Thread(target=wait)
    thread.start()
    time.sleep(0.02)

    command = sync_host.start("experiment-a", root=tmp_path)

    thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed == [command]
    assert command.clock == "monotonic_ns"
    assert command.start_at_monotonic_ns is not None
    assert (tmp_path / "experiment-a" / "start.json").exists()
    assert list((tmp_path / "experiment-a" / "ready").glob("*.json")) == []


def test_host_set_start_requires_delay_or_target(tmp_path):
    with pytest.raises(ValueError):
        sync_host.set_start("experiment-a", root=tmp_path)
    with pytest.raises(ValueError):
        soft_sync.set_start("experiment-a", root=tmp_path)
    with pytest.raises(ValueError):
        sync_host.set_start(
            "experiment-a",
            root=tmp_path,
            delay_s=0.1,
            target_ns=time.monotonic_ns(),
        )


def test_set_start_arms_but_does_not_signal_until_start(tmp_path):
    start_at = time.monotonic_ns() + 1_000_000_000

    pending = sync_host.set_start(
        "experiment-a",
        root=tmp_path,
        clock="monotonic_ns",
        target_ns=start_at,
    )

    assert pending.target_ns == start_at
    assert (tmp_path / "experiment-a" / "pending_start.json").exists()
    assert not (tmp_path / "experiment-a" / "start.json").exists()

    with pytest.raises(soft_sync.SoftSyncTimeout):
        sync_client.watch_start("experiment-a", root=tmp_path, timeout_s=0.01)

    observed = sync_host.start("experiment-a", root=tmp_path)

    assert observed.start_at_monotonic_ns == start_at
    assert observed.payload["sync_id"] == "experiment-a"
    assert observed.payload["target_ns"] == start_at
    assert observed.payload["armed"] is True
    assert observed.kwargs() == {"start_at_monotonic_ns": start_at}
    assert not (tmp_path / "experiment-a" / "pending_start.json").exists()


def test_watch_start_returns_boundary_without_sleeping_to_it(tmp_path):
    start_at = time.monotonic_ns() + 1_000_000_000
    sync_host.set_start("experiment-a", root=tmp_path, target_ns=start_at)
    sync_host.start("experiment-a", root=tmp_path)

    observed = sync_client.watch_start("experiment-a", root=tmp_path, timeout_s=0.1)

    assert observed.start_at_monotonic_ns == start_at
    assert observed.kwargs() == {"start_at_monotonic_ns": start_at}


def test_armed_delay_resolves_from_start_call(tmp_path):
    sync_host.set_start("experiment-a", root=tmp_path, delay_s=0.05)
    time.sleep(0.02)
    before_start = time.monotonic_ns()

    observed = sync_host.start("experiment-a", root=tmp_path)

    assert observed.payload["delay_s"] == 0.05
    assert observed.start_at_monotonic_ns is not None
    assert observed.start_at_monotonic_ns >= before_start + 45_000_000


def test_readiness_is_optional_but_available(tmp_path):
    sync = soft_sync.Coordinator("experiment-a", root=tmp_path)

    sync_client.sync_ready(
        "experiment-a",
        "directmuse-left",
        root=tmp_path,
        metadata={"device": "Muse-1234"},
    )

    ready = sync.wait_for_clients(count=1, timeout_s=0.1)

    assert [client["client_id"] for client in ready] == ["directmuse-left"]
    assert ready[0]["ready"]["metadata"]["device"] == "Muse-1234"


def test_sync_ready_and_get_clients_filter_stale_files(tmp_path):
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


def test_active_and_report_start_are_visible_to_host(tmp_path):
    sync_client.sync_ready("experiment-a", "client-a", root=tmp_path)
    sync_client.sync_active("experiment-a", "client-a", root=tmp_path)
    started = sync_client.report_start("experiment-a", "client-a", root=tmp_path)

    clients = sync_host.get_clients("experiment-a", root=tmp_path)

    assert clients[0]["client_id"] == "client-a"
    assert clients[0]["states"] == ["ready", "active", "started"]
    assert clients[0]["started"]["started_at_unix_ns"] == started["started_at_unix_ns"]


def test_sync_done_has_consistent_state_and_is_visible_to_host(tmp_path):
    done = sync_client.sync_done("experiment-a", "client-a", root=tmp_path)

    clients = sync_host.get_clients("experiment-a", root=tmp_path)

    assert done["state"] == "done"
    assert clients[0]["client_id"] == "client-a"
    assert clients[0]["states"] == ["done"]
    assert clients[0]["done"]["state"] == "done"


def test_stop_is_observed_by_client(tmp_path):
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


def test_cancel_after_start_interrupts_client_waiting_for_stop(tmp_path):
    observed: list[dict[str, object]] = []
    sync_host.start("experiment-a", root=tmp_path)

    def wait() -> None:
        try:
            sync_client.watch_stop(
                "experiment-a",
                client_id="client-a",
                root=tmp_path,
                timeout_s=1,
            )
        except soft_sync.SoftSyncCancelled as exc:
            observed.append(exc.payload)

    thread = threading.Thread(target=wait)
    thread.start()
    time.sleep(0.02)

    cancel = sync_host.cancel("experiment-a", root=tmp_path, reason="abort")

    thread.join(timeout=1)

    assert not thread.is_alive()
    assert observed == [cancel]


def test_reset_clears_state_but_keeps_sync_folder(tmp_path):
    sync_host.set_start("experiment-a", root=tmp_path, delay_s=0.1)
    sync_host.start("experiment-a", root=tmp_path)
    sync_client.sync_ready("experiment-a", "client-a", root=tmp_path)

    sync_host.reset("experiment-a", root=tmp_path)

    assert (tmp_path / "experiment-a").exists()
    assert (tmp_path / "experiment-a" / "session.json").exists()
    assert not (tmp_path / "experiment-a" / "start.json").exists()
    assert sync_host.get_clients("experiment-a", root=tmp_path) == []


def test_remove_deletes_sync_folder(tmp_path):
    sync_host.start("experiment-a", root=tmp_path)

    sync_host.remove("experiment-a", root=tmp_path)

    assert not (tmp_path / "experiment-a").exists()


def test_cancel_interrupts_client_wait(tmp_path):
    sync_host.cancel("experiment-a", root=tmp_path, reason="setup failed")

    with pytest.raises(soft_sync.SoftSyncCancelled) as exc:
        sync_client.sync_watch("experiment-a", root=tmp_path, timeout_s=0.1)

    assert "setup failed" in str(exc.value)


def test_get_clients_missing_sync_raises_without_creating_state(tmp_path):
    with pytest.raises(soft_sync.SoftSyncNotFound):
        sync_host.get_clients("missing", root=tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_status_missing_sync_raises_without_creating_state(tmp_path):
    with pytest.raises(soft_sync.SoftSyncNotFound):
        sync_host.status("missing", root=tmp_path)

    assert list(tmp_path.iterdir()) == []


def test_sync_ids_are_path_safe(tmp_path):
    with pytest.raises(ValueError):
        sync_host.start("../experiment-a", root=tmp_path)


def test_existing_start_requires_reset_or_overwrite(tmp_path):
    sync_host.start("experiment-a", root=tmp_path)

    with pytest.raises(soft_sync.SoftSyncStateError):
        sync_host.start("experiment-a", root=tmp_path)

    replacement = sync_host.start("experiment-a", root=tmp_path, overwrite=True)

    assert replacement.payload["schema"] == soft_sync.START_SCHEMA


def test_concurrent_start_has_one_winner(tmp_path):
    barrier = threading.Barrier(3)
    lock = threading.Lock()
    results: list[str] = []

    def start() -> None:
        barrier.wait(timeout=1)
        try:
            sync_host.start("experiment-a", root=tmp_path)
        except soft_sync.SoftSyncStateError:
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
    exit_code = main(["--root", str(tmp_path), "start", "../bad"])

    captured = capsys.readouterr()

    assert exit_code == 1
    assert "sync_id must match" in captured.err
    assert "Traceback" not in captured.err
    assert captured.out == ""
