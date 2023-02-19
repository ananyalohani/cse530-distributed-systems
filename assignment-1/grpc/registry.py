import socket
from urllib.parse import unquote
from concurrent import futures

import discord_pb2
import discord_pb2_grpc
import grpc


class RegistryServicer(discord_pb2_grpc.RegistryServicer):
    server_list = []

    def Register(self, request, context):
        server = request.server
        print(f"[.] JOIN REQUEST FROM {server.address}")
        if any(s.name == server.name for s in self.server_list):
            return discord_pb2.BaseResponse(
                status=discord_pb2.Status.ERROR,
                message=f"Server '{server.name}' already registered.",
            )
        self.server_list.append(server)
        return discord_pb2.BaseResponse(
            status=discord_pb2.Status.OK,
            message=f"Server '{server.name}' registered at {server.address}.",
        )

    def GetServerList(self, request, context):
        print(f"SERVER LIST REQUEST FROM {unquote(context.peer())}")
        return discord_pb2.GetServerListResponse(servers=self.server_list)


def serve():
    port = 0
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("", 0))
        port = s.getsockname()[1]
    server_address = f"[::]:{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    discord_pb2_grpc.add_RegistryServicer_to_server(RegistryServicer(), server)
    server.add_insecure_port(server_address)
    server.start()
    print(f"[.] Registry node started on {server_address}")
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
