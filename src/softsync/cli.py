from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from softsync import sync_client, sync_host
from softsync.core import SoftSyncCancelled, SoftSyncError, SoftSyncTimeout


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

    prepare = subparsers.add_parser("prepare", help="create sync state without starting")
    prepare.add_argument("sync_id")
    prepare.add_argument("--reset", action="store_true")

    set_start = subparsers.add_parser("set-start", help="arm a named start signal")
    set_start.add_argument("sync_id")
    set_start.add_argument(
        "--delay",
        type=float,
        default=None,
        help="future start delay in seconds",
    )
    set_start.add_argument("--target-ns", type=int, default=None, help="exact target timestamp")
    set_start.add_argument("--clock", choices=["monotonic_ns", "unix_ns"], default="monotonic_ns")
    set_start.add_argument("--overwrite", action="store_true")
    set_start.add_argument("--reset", action="store_true")

    start = subparsers.add_parser("start", help="publish a named start signal")
    start.add_argument("sync_id")
    start.add_argument("--clock", choices=["monotonic_ns", "unix_ns"], default="monotonic_ns")
    start.add_argument("--overwrite", action="store_true")
    start.add_argument("--reset", action="store_true")
    start.add_argument("--nonblocking", action="store_true")

    wait = subparsers.add_parser("wait", help="watch until a named start signal reaches its target")
    wait.add_argument("sync_id")
    wait.add_argument("--client-id")
    wait.add_argument("--timeout", type=float, default=None)
    wait.add_argument("--poll", type=float, default=0.005, help="fallback poll interval")

    ready = subparsers.add_parser("ready", help="notify host that one client is ready")
    ready.add_argument("sync_id")
    ready.add_argument("client_id")

    clients = subparsers.add_parser("clients", help="list client reports")
    clients.add_argument("sync_id")
    clients.add_argument("--max-age-ns", type=int, default=None, help="maximum report age in ns")

    active = subparsers.add_parser("active", help="notify host that one client is active")
    active.add_argument("sync_id")
    active.add_argument("client_id")

    report_start = subparsers.add_parser("report-start", help="report that one client started")
    report_start.add_argument("sync_id")
    report_start.add_argument("client_id")

    stop = subparsers.add_parser("stop", help="publish a stop signal")
    stop.add_argument("sync_id")
    stop.add_argument("--reason", default=None)
    stop.add_argument("--overwrite", action="store_true")

    cancel = subparsers.add_parser("cancel", help="publish a cancel signal")
    cancel.add_argument("sync_id")
    cancel.add_argument("--reason", default=None)
    cancel.add_argument("--overwrite", action="store_true")

    status = subparsers.add_parser("status", help="print flag status")
    status.add_argument("sync_id")

    reset = subparsers.add_parser("reset", help="clear run state for the named sync id")
    reset.add_argument("sync_id")

    remove = subparsers.add_parser("remove", help="delete the named sync directory")
    remove.add_argument("sync_id")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)

    try:
        if args.command == "prepare":
            _print_json(sync_host.prepare(args.sync_id, root=args.root, reset=args.reset))
            return 0

        if args.command == "set-start":
            command = sync_host.set_start(
                args.sync_id,
                root=args.root,
                delay_s=args.delay,
                target_ns=args.target_ns,
                clock=args.clock,
                overwrite=args.overwrite,
                reset=args.reset,
                nonblocking=args.nonblocking,
            )
            _print_json(command.payload)
            return 0

        if args.command == "start":
            command = sync_host.start(
                args.sync_id,
                root=args.root,
                clock=args.clock,
                overwrite=args.overwrite,
                reset=args.reset,
            )
            _print_json(command.payload)
            return 0

        if args.command == "wait":
            command = sync_client.sync_watch(
                args.sync_id,
                client_id=args.client_id,
                root=args.root,
                timeout_s=args.timeout,
                fallback_poll_interval_s=args.poll,
            )
            _print_json(command.payload)
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

        if args.command == "report-start":
            payload = sync_client.report_start(args.sync_id, args.client_id, root=args.root)
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
