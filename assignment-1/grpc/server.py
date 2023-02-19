import socket
from concurrent import futures

import discord_pb2
import discord_pb2_grpc
import grpc
import inquirer


class ServerServicer(discord_pb2_grpc.ServerServicer):
    clients = []
    articles = []
    MAX_CLIENTS = 10

    def __init__(self, server_name, server_address, registry_address):
        self.server_name = server_name
        self.server_address = server_address
        self.registry_address = registry_address

        print(
            f"[.] Registering server '{server_name}' at {server_address} with registry @ {registry_address}..."
        )
        with grpc.insecure_channel(self.registry_address) as channel:
            stub = discord_pb2_grpc.RegistryStub(channel)
            response = stub.Register(
                discord_pb2.RegisterRequest(
                    server=discord_pb2.ServerInfo(
                        name=self.server_name, address=self.server_address
                    )
                )
            )

            if response.status == discord_pb2.Status.ERROR:
                print(f"[*] {response.message}")
                print("FAIL")
                return

            print(f"[.] {response.message}")
            print("SUCCESS")

    def Join(self, request, context):
        print(f"[.] Join request received from client {request.clientId}")
        if len(self.clients) >= self.MAX_CLIENTS:
            return discord_pb2.BaseResponse(
                status=discord_pb2.Status.ERROR, message="Server is full."
            )
        if request.clientId in self.clients:
            return discord_pb2.BaseResponse(
                status=discord_pb2.Status.ERROR, message="Client already joined."
            )
        self.clients.append(request.clientId)
        return discord_pb2.BaseResponse(status=discord_pb2.Status.OK)

    def Leave(self, request, context):
        print(f"[.] Leave request received from client {request.clientId}")
        if request.clientId in self.clients:
            self.clients.remove(request.clientId)
            return discord_pb2.BaseResponse(status=discord_pb2.Status.OK)
        return discord_pb2.BaseResponse(
            status=discord_pb2.Status.ERROR, message="Client not found."
        )

    def GetArticles(self, request, context):
        print(f"[.] GetArticles request received from client {request.clientId}")
        print(
            f"[.] FOR type={'<BLANK>' if request.type == -1 else request.type}, author={request.author or '<BLANK>'}, time={request.time or '<BLANK>'}"
        )
        if request.clientId not in self.clients:
            return discord_pb2.BaseResponse(
                status=discord_pb2.Status.ERROR, message="Client not found."
            )
        filtered_articles = self.articles.copy()
        if request.type != -1:
            filtered_articles = [a for a in filtered_articles if a.type == request.type]
        if request.author != "":
            filtered_articles = [
                a for a in filtered_articles if a.author == request.author
            ]
        if request.time != 0:
            filtered_articles = [a for a in filtered_articles if a.time >= request.time]
        return discord_pb2.GetArticlesResponse(articles=filtered_articles)

    def PublishArticle(self, request, context):
        print(f"[.] PublishArticle request received from client {request.clientId}")
        if request.clientId not in self.clients:
            return discord_pb2.BaseResponse(
                status=discord_pb2.Status.ERROR, message="Client not found."
            )
        self.articles.append(request.article)
        return discord_pb2.BaseResponse(status=discord_pb2.Status.OK)


def serve(server_name, registry_address):
    port = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    server_address = f"[::]:{port}"
    discord_pb2_grpc.add_ServerServicer_to_server(
        ServerServicer(server_name, server_address, registry_address), server
    )
    server.add_insecure_port(server_address)
    server.start()
    print(f"[.] Server node started on {server_address}")
    server.wait_for_termination()


if __name__ == "__main__":
    questions = [
        inquirer.Text("server_name", message="Server name"),
        inquirer.Text("registry_address", message="Registry address"),
    ]

    answers = inquirer.prompt(questions)
    server_name = answers["server_name"]
    registry_address = answers["registry_address"]

    serve(server_name, registry_address)
