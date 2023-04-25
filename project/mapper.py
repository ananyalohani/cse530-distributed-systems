from typing import List
import socket
from concurrent import futures

import grpc
import map_reduce_pb2_grpc


class Mapper(map_reduce_pb2_grpc.MapperServicer):
    id: int = 0
    address: str = ""
    filepaths: List[str] = []
    reducers: List[str] = []
    datastore: dict = {}

    def __init__(self, id: int, filepaths: List[str] = [],
                 reducers: List[str] = []):
        self.id = id
        self.filepaths = filepaths
        self.reducers = reducers

    def serve(self, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", port))
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.address = f"[::]:{port}"
        map_reduce_pb2_grpc.add_MapperServicer_to_server(self, self.server)
        self.server.add_insecure_port(self.address)
        self.server.start()
        print(f"[.] Mapper {self.id} node started on {self.address}")

    def Map(self, request, context):
        pass

    def shuffle(self):
        pass
