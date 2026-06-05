import time

from softsync import SoftSyncCancelled, SoftSyncNotFound, sync_client

SYNC_ID = "softsync-example"
CLIENT_ID = "client-01"

print("client: waiting for host")
while True:
    try:
        sync_client.wait_for_ready_request(SYNC_ID, client_id=CLIENT_ID, timeout_s=1)
        break
    except (SoftSyncCancelled, SoftSyncNotFound):
        time.sleep(0.5)

sync_client.sync_ready(SYNC_ID, CLIENT_ID)
print("client: ready")

sync_client.wait_for_start(SYNC_ID, client_id=CLIENT_ID)
print("client: started")

sync_client.sync_active(SYNC_ID, CLIENT_ID)
sync_client.watch_stop(SYNC_ID, client_id=CLIENT_ID)
sync_client.sync_done(SYNC_ID, CLIENT_ID)
print("client: done")
