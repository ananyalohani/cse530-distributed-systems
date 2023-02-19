import zmq
import json

REGISTRY_PORT = 5555
MAX_SERVERS = 10

servers = []

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind(f"tcp://*:{REGISTRY_PORT}")

print(f"[x] Registry listening on localhost:{REGISTRY_PORT}")

while True:
    payload = socket.recv()
    payload = payload.decode('utf-8')
    payload = json.loads(payload)
    client = None
    if 'client_id' in payload:
        client = payload['client_id']
    elif 'server_id' in payload:
        client = payload['server_id']

    if payload['method'] == "Register":
        if len(servers) == MAX_SERVERS:
            print("[.] MAX_SERVERS reached, rejecting server %r",
                  payload['server_id'])
            socket.send(b"FAILURE")
            continue
        if payload['server_id'] in servers:
            print("[.] Server %r already registered", payload['server_id'])
            socket.send(b"SUCCESS")
            continue
        print(
            f"[.] Registering server {payload['server_id']} on port {payload['params']['port']}")
        servers.append(
            {payload['server_id']: payload['params']['port']})
        socket.send(b"SUCCESS")

    elif payload['method'] == "GetServerList":
        print(f"[.] Returning server list to {payload['client_id']}")
        socket.send(json.dumps(servers, default=tuple).encode('utf-8'))

    else:
        socket.send(b"INVALID REQUEST")
