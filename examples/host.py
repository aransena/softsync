import time

from softsync import sync_host

SYNC_ID = "softsync-example"
CLIENT_ID = "client-01"

sync_host.prepare(SYNC_ID, reset=True, wait_for_clients=[CLIENT_ID])
print("host: waiting for client")

signal = sync_host.start(SYNC_ID)
print(f"host: start published at {signal.start_at_monotonic_ns}")

time.sleep(2)
print(sync_host.get_clients(SYNC_ID))

sync_host.stop(SYNC_ID, reason="example complete")
print("host: stopped")
