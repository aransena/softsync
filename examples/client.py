from softsync import SoftSyncCancelled, sync_client

SYNC_ID = "softsync-example"
CLIENT_ID = "client-01"

print("client: waiting for host")
try:
    sync_client.wait_for_ready_request(SYNC_ID)
except SoftSyncCancelled as exc:
    raise SystemExit(f"client: sync cancelled before ready request: {exc}") from exc

sync_client.sync_ready(SYNC_ID, CLIENT_ID)
print("client: ready")

sync_client.wait_for_start(SYNC_ID, client_id=CLIENT_ID)
print("client: started")

sync_client.sync_active(SYNC_ID, CLIENT_ID)
sync_client.watch_stop(SYNC_ID, client_id=CLIENT_ID)
sync_client.sync_done(SYNC_ID, CLIENT_ID)
print("client: done")
