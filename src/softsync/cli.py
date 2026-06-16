from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from softsync import sync_client, sync_host
from softsync.core import SoftSyncCancelled, SoftSyncError, SoftSyncTimeout, validate_uc_descriptors


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, indent=2, sort_keys=True, default=str))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="softsync")
    parser.add_argument(
        "--root",
        type=Path,
        default=None,
        help="root folder, default /tmp/softsync",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser("prepare", help="configure sync state without starting")
    prepare.add_argument("sync_id")
    prepare.add_argument("--reset", action="store_true")
    prepare.add_argument("--delay-ns", type=int, default=None, help="delay after readiness in ns")
    prepare.add_argument("--timestamp-ns", type=int, default=None, help="monotonic start timestamp")
    prepare.add_argument(
        "--wait-for-n-clients",
        type=int,
        default=None,
        help="number of ready clients required before start",
    )
    prepare.add_argument(
        "--wait-for-client",
        action="append",
        dest="wait_for_clients",
        default=None,
        help="specific client_id required before start; may be repeated",
    )

    start = subparsers.add_parser("start", help="publish start after prepared conditions")
    start.add_argument("sync_id")
    start.add_argument("--verbose", action="store_true")

    wait = subparsers.add_parser("wait", help="wait until start.json exists")
    wait.add_argument("sync_id")
    wait.add_argument("--client-id")
    wait.add_argument("--timeout", type=float, default=None)
    wait.add_argument("--poll", type=float, default=0.005, help="fallback poll interval")

    wait_ready = subparsers.add_parser(
        "wait-ready",
        help="wait until the host requests ready clients",
    )
    wait_ready.add_argument("sync_id")
    wait_ready.add_argument("--timeout", type=float, default=None)
    wait_ready.add_argument("--poll", type=float, default=0.005, help="fallback poll interval")

    discover_ready = subparsers.add_parser(
        "discover-ready",
        help="list sync IDs for hosts currently requesting ready clients",
    )

    ready = subparsers.add_parser("ready", help="notify host that one client is ready")
    ready.add_argument("sync_id")
    ready.add_argument("client_id")

    clients = subparsers.add_parser("clients", help="list client reports")
    clients.add_argument("sync_id")
    clients.add_argument("--max-age-ns", type=int, default=None, help="maximum report age in ns")

    active = subparsers.add_parser("active", help="refresh one client's active heartbeat")
    active.add_argument("sync_id")
    active.add_argument("client_id")

    done = subparsers.add_parser("done", help="notify host that one client is done")
    done.add_argument("sync_id")
    done.add_argument("client_id")

    stop = subparsers.add_parser("stop", help="publish a stop signal and clean active state")
    stop.add_argument("sync_id")
    stop.add_argument("--reason", default=None)
    stop.add_argument("--overwrite", action="store_true")

    cancel = subparsers.add_parser("cancel", help="publish a cancel signal and clean active state")
    cancel.add_argument("sync_id")
    cancel.add_argument("--reason", default=None)
    cancel.add_argument("--overwrite", action="store_true")

    status = subparsers.add_parser("status", help="print sync status")
    status.add_argument("sync_id")

    validate = subparsers.add_parser(
        "validate",
        help="validate packaged UC descriptors or one sync directory",
    )
    validate.add_argument("sync_id", nargs="?")

    reset = subparsers.add_parser("reset", help="clear run state for the named sync id")
    reset.add_argument("sync_id")

    remove = subparsers.add_parser("remove", help="delete the named sync directory")
    remove.add_argument("sync_id")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        if args.command == "prepare":
            _print_json(
                sync_host.prepare(
                    args.sync_id,
                    root=args.root,
                    reset=args.reset,
                    delay_ns=args.delay_ns,
                    timestamp_ns=args.timestamp_ns,
                    wait_for_N_clients=args.wait_for_n_clients,
                    wait_for_clients=args.wait_for_clients,
                )
            )
            return 0

        if args.command == "start":
            command = sync_host.start(
                args.sync_id,
                root=args.root,
                verbose=args.verbose,
            )
            _print_json(command.payload)
            return 0

        if args.command == "wait":
            command = sync_client.wait_for_start(
                args.sync_id,
                client_id=args.client_id,
                root=args.root,
                timeout_s=args.timeout,
                fallback_poll_interval_s=args.poll,
            )
            _print_json(command.payload)
            return 0

        if args.command == "wait-ready":
            payload = sync_client.wait_for_ready_request(
                args.sync_id,
                root=args.root,
                timeout_s=args.timeout,
                fallback_poll_interval_s=args.poll,
            )
            _print_json(payload)
            return 0

        if args.command == "discover-ready":
            payload = sync_client.discover_ready_sync_ids(root=args.root)
            _print_json(payload)
            return 0

        if args.command == "ready":
            payload = sync_client.sync_ready(args.sync_id, args.client_id, root=args.root)
            _print_json(payload)
            return 0

        if args.command == "clients":
            _print_json(
                sync_host.get_clients(args.sync_id, root=args.root, max_age_ns=args.max_age_ns)
            )
            return 0

        if args.command == "active":
            payload = sync_client.sync_active(args.sync_id, args.client_id, root=args.root)
            _print_json(payload)
            return 0

        if args.command == "done":
            payload = sync_client.sync_done(args.sync_id, args.client_id, root=args.root)
            _print_json(payload)
            return 0

        if args.command == "stop":
            payload = sync_host.stop(
                args.sync_id,
                root=args.root,
                reason=args.reason,
                overwrite=args.overwrite,
            )
            _print_json(payload)
            return 0

        if args.command == "cancel":
            payload = sync_host.cancel(
                args.sync_id,
                root=args.root,
                reason=args.reason,
                overwrite=args.overwrite,
            )
            _print_json(payload)
            return 0

        if args.command == "status":
            _print_json(sync_host.status(args.sync_id, root=args.root))
            return 0

        if args.command == "validate":
            if args.sync_id:
                payload = sync_host.validate(args.sync_id, root=args.root)
            else:
                payload = validate_uc_descriptors()
            _print_json(payload)
            return 0 if payload.get("results", {}).get("status") == "passed" else 1

        if args.command == "reset":
            sync_host.reset(args.sync_id, root=args.root)
            return 0

        if args.command == "remove":
            sync_host.remove(args.sync_id, root=args.root)
            return 0

    except SoftSyncCancelled as exc:
        _print_json({"error": "cancelled", "payload": exc.payload})
        return 2
    except SoftSyncTimeout as exc:
        print(str(exc), file=sys.stderr)
        return 3
    except (SoftSyncError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
