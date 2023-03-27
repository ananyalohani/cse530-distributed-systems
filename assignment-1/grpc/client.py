import uuid
import time
from datetime import datetime

import discord_pb2
import discord_pb2_grpc
import grpc
import inquirer


class Client:
    def __init__(self):
        self.client_id = str(uuid.uuid4())

    def join(self):
        with grpc.insecure_channel(self.registry_address) as channel:
            stub = discord_pb2_grpc.RegistryStub(channel)
            response = stub.GetServerList(discord_pb2.GetServerListRequest())
            servers = response.servers

            if len(servers) == 0:
                print("No servers available.")
                return

            for server in servers:
                print(f"{server.name} - {server.address}")

            questions = [
                inquirer.List(
                    "server",
                    message="Select a server",
                    choices=[s.name for s in servers],
                )
            ]
            answers = inquirer.prompt(questions)
            server = next(s for s in servers if s.name == answers["server"])
            self.server_name = server.name
            self.server_address = server.address

        with grpc.insecure_channel(self.server_address) as channel:
            stub = discord_pb2_grpc.ServerStub(channel)
            response = stub.Join(discord_pb2.JoinServerRequest(clientId=self.client_id))
            if response.status == discord_pb2.Status.ERROR:
                print(f"[*] {response.message}")
                print("FAIL")
                return
            print("SUCCESS")

    def leave(self):
        with grpc.insecure_channel(self.server_address) as channel:
            stub = discord_pb2_grpc.ServerStub(channel)
            response = stub.Leave(
                discord_pb2.LeaveServerRequest(clientId=self.client_id)
            )
            if response.status == discord_pb2.Status.ERROR:
                print(f"[*] {response.message}")
                print("FAIL")
                return
            print("SUCCESS")

    def get_articles(self):
        questions = [
            inquirer.Text("author", message="Filter by author"),
            inquirer.List(
                "type",
                message="Filter by type",
                choices=["All", "SPORTS", "FASHION", "POLITICS"],
            ),
            inquirer.Text("time", message="Filter by time"),
        ]
        answers = inquirer.prompt(questions)
        time = (
            int(datetime.strptime(answers["time"], "%d/%m/%Y").timestamp())
            if answers["time"]
            else 0
        )
        article_type = (
            -1
            if answers["type"] == "All"
            else getattr(discord_pb2.ArticleType, answers["type"])
        )
        with grpc.insecure_channel(self.server_address) as channel:
            stub = discord_pb2_grpc.ServerStub(channel)
            response = stub.GetArticles(
                discord_pb2.GetArticlesRequest(
                    clientId=self.client_id,
                    author=answers["author"] or None,
                    type=article_type,
                    time=time,
                )
            )
            print(f"Articles from '{self.server_name}' server:")
            for article in response.articles:
                print("Title:", article.title)
                print("Author:", article.author)
                print("Content:", article.content)
                print("Type:", discord_pb2.ArticleType.Name(article.type))
                print("Time:", datetime.fromtimestamp(article.time))
                print()

    def publish_article(self):
        questions = [
            inquirer.Text("title", message="Title"),
            inquirer.Text("author", message="Author"),
            inquirer.Text("content", message="Content"),
            inquirer.List(
                "type", message="Type", choices=["SPORTS", "FASHION", "POLITICS"]
            ),
        ]
        answers = inquirer.prompt(questions)
        if (
            not answers["title"]
            or not answers["author"]
            or not answers["content"]
            or not answers["type"]
        ):
            print("All fields are required.")
            return
        with grpc.insecure_channel(self.server_address) as channel:
            stub = discord_pb2_grpc.ServerStub(channel)
            response = stub.PublishArticle(
                discord_pb2.PublishArticleRequest(
                    clientId=self.client_id,
                    article=discord_pb2.Article(
                        title=answers["title"],
                        author=answers["author"],
                        content=answers["content"],
                        type=getattr(discord_pb2.ArticleType, answers["type"]),
                        time=int(time.time()),
                    ),
                )
            )
            if response.status == discord_pb2.Status.ERROR:
                print("[!] Error while publishing article: " + response.message)
            else:
                print("[.] Article published successfully.")

    def start(self):
        registry_address = inquirer.prompt(
            [inquirer.Text("registry_address", message="Registry address")]
        )["registry_address"]
        self.registry_address = registry_address

        actions = [
            ("Join", self.join),
            ("Leave", self.leave),
            ("Get articles", self.get_articles),
            ("Publish article", self.publish_article),
        ]

        while True:
            answers = inquirer.prompt(
                [
                    inquirer.List(
                        "action",
                        message="Select an action",
                        choices=[a[0] for a in actions],
                    ),
                ]
            )
            action = next(a for a in actions if a[0] == answers["action"])
            action[1]()


if __name__ == "__main__":
    client = Client()
    client.start()
