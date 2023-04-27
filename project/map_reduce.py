import multiprocessing
import os
import socket
import time
from abc import ABC, abstractmethod
from collections import defaultdict
from concurrent import futures
from contextlib import closing
from typing import List

import grpc
import map_reduce_pb2
import map_reduce_pb2_grpc


class Mapper(map_reduce_pb2_grpc.MapperServicer, ABC):
    id: int = 0
    address: str = ""
    filepaths: List[str] = []
    datastore: defaultdict = defaultdict(int)
    shard_filepaths = set()

    def __init__(self, id: int, filepaths: List[str] = [], reducers: List[str] = []):
        self.id = id
        self.filepaths = filepaths
        self.reducers = reducers

    @abstractmethod
    def Map(self, request: map_reduce_pb2.MapRequest, context: grpc.ServicerContext):
        pass

    @abstractmethod
    def _map(self, key: any, value: any):
        pass

    def serve(self, port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", port))
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        self.address = f"[::]:{port}"
        map_reduce_pb2_grpc.add_MapperServicer_to_server(self, self.server)
        self.server.add_insecure_port(self.address)
        self.server.start()
        print(f"[.] Mapper {self.id} node started on {self.address}")


class Reducer(map_reduce_pb2_grpc.ReducerServicer, ABC):
    id: int = 0
    datastore: defaultdict = defaultdict(list)
    address: str = ""

    def __init__(self, id: int):
        self.id = id

    @abstractmethod
    def Reduce(
        self, request: map_reduce_pb2.ReduceRequest, context: grpc.ServicerContext
    ):
        pass

    @abstractmethod
    def _reduce(self, key: any, value: any):
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


class Manager(ABC):
    datastore = defaultdict(int)
    map_filepaths = []

    def __init__(
        self,
        operation: str,
        config_path: str,
        input_paths: List[str],
        MapperClass,
        ReducerClass,
    ):
        self.operation = operation
        with open(config_path, "r") as f:
            lines = f.readlines()
        self.num_mappers = int(lines[0].split(" = ")[1])
        self.num_reducers = int(lines[1].split(" = ")[1])
        self.files_per_mapper = 0
        if self.num_mappers < len(input_paths):
            self.files_per_mapper = len(input_paths) // self.num_mappers
        else:
            self.files_per_mapper = 1

        if os.uname().sysname == "Darwin":
            multiprocessing.set_start_method("spawn")

        self.input_paths = input_paths
        self.shards = [[] for _ in range(self.num_reducers)]

        self.mappers = []
        self.mapper_processes = []
        self.mapper_addresses = []
        self.reducers = []
        self.reducer_processes = []
        self.reducer_addresses = []
        for i in range(self.num_mappers):
            idx = (
                (i + 1) * self.files_per_mapper
                if (i + 1) * self.files_per_mapper < len(self.input_paths)
                else len(self.input_paths)
            )
            mapper = MapperClass(
                i + 1, self.input_paths[i * self.files_per_mapper : idx]
            )
            self.mappers.append(mapper)

        for i in range(self.num_reducers):
            reducer = ReducerClass(i + 1)
            self.reducers.append(reducer)

    def cleanup(self):
        for type in ["map", "reduce"]:
            folder = os.path.join(
                os.path.dirname(__file__), "sample", self.operation, type
            )
            try:
                for filename in os.listdir(folder):
                    filepath = os.path.join(folder, filename)
                    if os.path.isfile(filepath):
                        os.unlink(filepath)
            except Exception as e:
                print("Failed to delete:", e)

    def find_free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def start_process(self, obj, idx, port):
        p = None
        if obj == "mapper":
            p = multiprocessing.Process(target=self.start_mapper, args=(idx, port))
            p.start()
        elif obj == "reducer":
            p = multiprocessing.Process(target=self.start_reducer, args=(idx, port))
            p.start()
        return p

    def start_mapper(self, idx, port):
        self.mappers[idx].serve(port)
        self.mappers[idx].server.wait_for_termination()

    def start_reducer(self, idx, port):
        self.reducers[idx].serve(port)
        self.reducers[idx].server.wait_for_termination()

    def get_map_response(self, future: grpc.Future):
        self.map_filepaths = future.result().filepaths

    def run(self):
        self.cleanup()
        time.sleep(1)
        for i in range(len(self.mappers)):
            if not len(self.mappers[i].filepaths):
                continue
            with grpc.insecure_channel(self.mapper_addresses[i]) as channel:
                stub = map_reduce_pb2_grpc.MapperStub(channel)
                response = stub.Map.future(
                    map_reduce_pb2.MapRequest(
                        num_reducers=len(self.reducers),
                    )
                )
                response.add_done_callback(self.get_map_response)
                time.sleep(2)

        for i in range(len(self.reducers)):
            with grpc.insecure_channel(self.reducer_addresses[i]) as channel:
                stub = map_reduce_pb2_grpc.ReducerStub(channel)
                response = stub.Reduce.future(
                    map_reduce_pb2.ReduceRequest(
                        filepath=self.map_filepaths[i],
                    )
                )
                time.sleep(2)
