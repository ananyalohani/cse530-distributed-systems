from typing import List
import socket
from concurrent import futures
from google.protobuf.struct_pb2 import Struct

import grpc

PORT = 50051


class Mapper():
    filepaths: List[str] = []
    reducers: List[str] = []
    datastore: List[Struct] = []

    def __init__(self, filepaths: List[str] = [], reducers: List[str] = []):
        self.filepaths = filepaths
        self.reducers = reducers
        pass

    def serve(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", PORT))
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.address = f"[::]:{PORT}"
        # pkda_pb2_grpc.add_PKDAServicer_to_server(self, self.server)
        self.server.add_insecure_port(self.address)
        self.server.start()
        print(f"[.] PKDA node started on {self.address}")

    def map(self):
        pass

    def shuffle(self):
        pass
