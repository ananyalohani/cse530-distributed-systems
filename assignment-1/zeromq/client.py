import zmq
import inquirer
import json
import uuid
import datetime

CLIENT_ID = str(uuid.uuid4())
REGISTRY_PORT = 5555


class Client:
    def __init__(self):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REQ)
        self.socket.connect(f"tcp://localhost:{REGISTRY_PORT}")

    def join(self):
        payload = {
            "method": "GetServerList",
            "client_id": CLIENT_ID,
        }
        self.socket.send(json.dumps(payload).encode("utf-8"))
        response = self.socket.recv().decode("utf-8")
        servers = json.loads(response)
        for server in servers:
            print(f"{server['server_id']} localhost:{server['port']}")
        print()
        answers = inquirer.prompt(
            [
                inquirer.List(
                    "server",
                    message="Select a server",
                    choices=[server["server_id"] for server in servers],
                )
            ]
        )
        server_id = answers["server"]
        server_port = next(
            server["port"] for server in servers if server["server_id"] == server_id
        )
        self.server_id = server_id
        self.server_port = server_port
        self.server_socket = self.context.socket(zmq.REQ)
        self.server_socket.connect(f"tcp://localhost:{server_port}")
        response = self.server_socket.send(
            json.dumps(
                {
                    "client_id": CLIENT_ID,
                    "method": "JoinServer",
                }
            ).encode("utf-8")
        )
        response = self.server_socket.recv().decode("utf-8")
        print(response)

    def leave(self):
        self.server_socket.send(
            json.dumps(
                {
                    "client_id": CLIENT_ID,
                    "method": "LeaveServer",
                }
            ).encode("utf-8")
        )
        response = self.server_socket.recv().decode("utf-8")
        print(response)

    def publish_article(self):
        questions = [
            inquirer.Text("author", message="Author"),
            inquirer.Text("content", message="Content"),
            inquirer.List(
                "type", message="Type", choices=["SPORTS", "FASHION", "POLITICS"]
            ),
        ]
        answers = inquirer.prompt(questions)
        if not answers["author"] or not answers["content"] or not answers["type"]:
            print("All fields are required.")
            return
        self.server_socket.send(
            json.dumps(
                {
                    "client_id": CLIENT_ID,
                    "method": "PublishArticle",
                    "params": {
                        "author": answers["author"],
                        "content": answers["content"],
                        "type": answers["type"],
                    },
                }
            ).encode("utf-8")
        )
        response = self.server_socket.recv().decode("utf-8")
        print(response)

    def get_articles(self):
        questions = [
            inquirer.Text("author", message="Filter by author"),
            inquirer.List(
                "type",
                message="Filter by type",
                choices=["All", "SPORTS", "FASHION", "POLITICS"],
            ),
            inquirer.Text("date", message="Filter by date"),
        ]
        answers = inquirer.prompt(questions)
        self.server_socket.send(
            json.dumps(
                {
                    "client_id": CLIENT_ID,
                    "method": "GetArticles",
                    "params": {
                        "author": answers["author"],
                        "type": answers["type"],
                        "date": answers["date"],
                    },
                }
            ).encode("utf-8")
        )
        response = self.server_socket.recv().decode("utf-8")
        try:
            articles = json.loads(response)
            for article in articles:
                print("Author:", article["author"])
                print("Content:", article["content"])
                print("Type:", article["type"])
                print("Time:", datetime.datetime.fromtimestamp(article["timestamp"]))
                print()
        except json.decoder.JSONDecodeError:
            print(response)

    def start(self):
        while True:
            q = [
                inquirer.List(
                    "method",
                    message="What do you want to do?",
                    choices=["Join", "Leave", "Publish Article", "Get Articles"],
                )
            ]
            answers = inquirer.prompt(q, theme=inquirer.themes.BlueComposure())
            method = answers["method"]
            if method == "Join":
                self.join()
            elif method == "Leave":
                self.leave()
            elif method == "Publish Article":
                self.publish_article()
            elif method == "Get Articles":
                self.get_articles()


if __name__ == "__main__":
    client = Client()
    client.start()
