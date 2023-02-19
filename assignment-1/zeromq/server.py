import time
from datetime import datetime
import zmq
import inquirer
import uuid
import json

REGISTRY_PORT = 5555
MAX_CLIENTS = 5

context = zmq.Context()
socket = context.socket(zmq.REQ)
q = [
    inquirer.Text("port", message="Enter port number", validate=lambda _, x: x != ""),
    inquirer.Text("server_id", message="Enter server id"),
]
answers = inquirer.prompt(q, theme=inquirer.themes.BlueComposure())
server_id = answers["server_id"] if answers["server_id"] != "" else str(uuid.uuid4())
port = answers["port"]

# Send register request to registry
socket.connect(f"tcp://localhost:{5555}")
payload = {
    "method": "Register",
    "params": {"port": port},
    "server_id": server_id,
}
print("[x] Requesting server registration")
socket.send(json.dumps(payload).encode("utf-8"))
response = socket.recv().decode("utf-8")
print(f"[.] Got {response}")
if response != "SUCCESS":
    exit()

# Bind to port
socket = context.socket(zmq.REP)
socket.bind(f"tcp://*:{port}")
print(f"[x] Server {server_id} listening on localhost:{port}")
articles = []
clientele = set()

# payload = {
#     method: 'JoinServer' | 'LeaveServer' | 'PublishArticle' | 'GetArticles',
#     client_id: 'client_id',
#     params: {
#         ...
#     }
# }

while True:
    #  Wait for next request from client
    payload = socket.recv()
    payload = payload.decode("utf-8")
    payload = json.loads(payload)

    method = payload["method"]
    client_id = payload["client_id"]
    params = payload["params"] if "params" in payload else {}

    print(f"[.] {method} request from {client_id}")

    if method == "JoinServer":
        if len(clientele) == MAX_CLIENTS:
            print("[.] MAX_CLIENTS reached, rejecting client %r", client_id)
            socket.send(b"FAILURE")
            continue
        if client_id in clientele:
            print("[.] Client %r already registered", client_id)
            socket.send(b"SUCCESS")
            continue
        print(f"[.] Registering client {client_id}")
        clientele.add(client_id)
        socket.send(b"SUCCESS")

    elif method == "LeaveServer":
        if client_id not in clientele:
            print("[.] Client %r not registered", client_id)
            socket.send(b"FAILURE")
            continue
        print(f"[.] Unregistering client {client_id}")
        clientele.remove(client_id)
        socket.send(b"SUCCESS")

    elif method == "PublishArticle":
        if client_id not in clientele:
            socket.send(b"FAILURE")
            continue
        article = {
            "type": params["type"],
            "author": params["author"],
            "content": params["content"],
            "timestamp": time.time(),
        }
        articles.append(article)
        socket.send(b"SUCCESS")

    elif method == "GetArticles":
        if client_id not in clientele:
            socket.send(b"FAILURE")
            continue
        if "date" not in params:
            socket.send(b"INVALID REQUEST")
            continue
        date = params["date"]
        timestamp = time.mktime(datetime.strptime(date, "%d/%m/%Y").timetuple())
        result = [x for x in articles if x["timestamp"] >= timestamp]
        if "type" in params and params["type"] != "All":
            result = [x for x in result if x["type"] == params["type"]]
        if "author" in params and params["author"] != "":
            result = [x for x in result if x["author"] == params["author"]]
        socket.send(json.dumps(list(result)).encode("utf-8"))

    else:
        socket.send(b"INVALID REQUEST")
