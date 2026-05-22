from __future__ import annotations

import argparse

from softsync.core import _publish_scheduled_start


def main() -> int:
    parser = argparse.ArgumentParser(prog="python -m softsync._start_publisher")
    parser.add_argument("sync_id")
    parser.add_argument("root")
    args = parser.parse_args()
    _publish_scheduled_start(args.sync_id, root=args.root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
