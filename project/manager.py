from typing import List
import os
import multiprocessing
import socket
from contextlib import closing

import grpc
import map_reduce_pb2
import map_reduce_pb2_grpc


class Manager():
    def __init__(self, config_path: str, input_paths: List[str]):
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
            multiprocessing.set_start_method('spawn')

        self.input_paths = input_paths

        self.mappers = []
        self.mapper_processes = []
        self.mapper_addresses = []
        self.reducers = []
        self.reducer_processes = []
        self.reducer_addresses = []
        self.initialize_map_reduce()

    def initialize_map_reduce(self):
        pass

    def find_free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def start_process(self, obj, idx, port):
        p = None
        if obj == 'mapper':
            p = multiprocessing.Process(
                target=self.start_mapper, args=(idx, port))
            p.start()
        elif obj == 'reducer':
            p = multiprocessing.Process(
                target=self.start_reducer, args=(idx, port))
            p.start()
        return p

    def start_mapper(self, idx, port):
        self.mappers[idx].serve(port)
        self.mappers[idx].server.wait_for_termination()

    def start_reducer(self, idx, port):
        self.reducers[idx].serve(port)
        self.reducers[idx].server.wait_for_termination()

    def run(self):
        for i in range(len(self.mappers)):
            if (not len(self.mappers[i].filepaths)):
                continue
            with grpc.insecure_channel(self.mapper_addresses[i]) as channel:
                stub = map_reduce_pb2_grpc.MapperStub(channel)
                stub.Map(
                    map_reduce_pb2.MapRequest(
                        reducers=self.reducer_addresses,
                    )
                )
