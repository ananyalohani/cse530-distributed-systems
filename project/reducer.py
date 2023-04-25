from typing import List
import socket
from concurrent import futures

import grpc
import map_reduce_pb2_grpc


class Reducer(map_reduce_pb2_grpc.ReducerServicer):
    id: int = 0
    datastore: dict = {}
    address: str = ""
    mappers: List[str] = []

    def __init__(self, id: int):
        self.id = id

    def Reduce(self, request, context):
        pass

    def serve(self, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", port))
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.address = f"[::]:{port}"
        map_reduce_pb2_grpc.add_ReducerServicer_to_server(self, self.server)
        self.server.add_insecure_port(self.address)
        self.server.start()
        print(f"[.] Reducer {self.id} node started on {self.address}")
