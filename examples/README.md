# softsync examples

Run the host in one terminal:

```bash
PYTHONPATH=src python examples/host.py
```

Run the client in another terminal:

```bash
PYTHONPATH=src python examples/client.py
```

The host prepares `softsync-example`, writes `waiting_for_clients.json` while it
waits for `client-01`, publishes `start.json`, waits briefly, then stops the
run.

The client can be started before the host. It waits until the host prepares the
next run and requests ready clients.
