from multiprocessing import Process
import multiprocessing
from typing import List
import os
import time
import json
import socket
from contextlib import closing
from collections import defaultdict

from mapper import Mapper
from reducer import Reducer

import grpc
import map_reduce_pb2_grpc
import map_reduce_pb2


class WordCountMapper(Mapper):
    def Map(self, request, context):
        lines = []
        self.datastore = defaultdict(int)
        for filepath in self.filepaths:
            with open(filepath, "r") as input_file:
                for line in input_file:
                    for word in line.strip().split():
                        word = word.lower()
                        self.datastore[word] += 1
        responses = []
        shards = self.sort(len(request.reducers))
        for i, reducer in enumerate(request.reducers):
            response = None
            with grpc.insecure_channel(reducer) as channel:
                stub = map_reduce_pb2_grpc.ReducerStub(channel)
                response = stub.Reduce(
                    map_reduce_pb2.ReduceRequest(
                        mapper=self.address,
                        datastore=json.dumps(shards[i])
                    )
                )
                responses.append(json.loads(response.datastore))
        final_store = self.shuffle(responses)
        return map_reduce_pb2.MapResponse(datastore=json.dumps(final_store), reducers=self.reducers)

    def sort(self, num_reducers: int):
        shards = [defaultdict(int) for _ in range(num_reducers)]
        for word, count in self.datastore.items():
            shard_index = abs(hash(word)) % num_reducers
            shards[shard_index][word] += count
        return shards

    def shuffle(self, responses):
        final_store = defaultdict(int)
        for response in responses:
            for key, value in response.items():
                final_store[key] += value
        return final_store


class WordCountReducer(Reducer):
    def Reduce(self, request, context):
        datastore = json.loads(request.datastore)
        for word, count in datastore.items():
            self.datastore[word] += count
        with open(os.path.join(os.path.dirname(__file__), f"sample/word_count/wc_output{self.id}.txt"), "w") as f:
            for key, value in self.datastore.items():
                f.write(f"{key} {value}\n")
        return map_reduce_pb2.ReduceResponse(
            datastore=json.dumps(self.datastore),
            reducer=self.address
        )


class WordCountManager():
    datastore = {}

    def __init__(self, config_path: str, input_paths: List[str]):
        with open(config_path, "r") as f:
            lines = f.readlines()
        self.num_mappers = int(lines[0].split(" = ")[1])
        self.num_reducers = int(lines[1].split(" = ")[1])
        files_per_mapper = 0
        if self.num_mappers < len(input_paths):
            files_per_mapper = len(input_paths) // self.num_mappers
        else:
            files_per_mapper = 1

        if os.uname().sysname == "Darwin":
            multiprocessing.set_start_method('spawn')

        self.mappers = []
        self.mapper_processes = []
        self.mapper_addresses = []
        for i in range(self.num_mappers):
            idx = (i + 1) * files_per_mapper if (i + 1) * \
                files_per_mapper < len(input_paths) else len(input_paths)
            mapper = WordCountMapper(
                i + 1, input_paths[i * files_per_mapper:idx])
            self.mappers.append(mapper)

        self.reducers = []
        self.reducer_processes = []
        self.reducer_addresses = []
        for i in range(self.num_reducers):
            reducer = WordCountReducer(i + 1)
            self.reducers.append(reducer)

    def find_free_port(self):
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(('', 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]

    def start_process(self, obj, idx, port):
        if obj == 'mapper':
            p = Process(target=self.start_mapper, args=(idx, port))
            p.start()
        elif obj == 'reducer':
            p = Process(target=self.start_reducer, args=(idx, port))
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


if __name__ == "__main__":
    config = os.path.join(os.path.dirname(__file__),
                          "sample/word_count/config.txt")
    input1 = os.path.join(os.path.dirname(__file__),
                          "sample/word_count/Input1.txt")
    input2 = os.path.join(os.path.dirname(__file__),
                          "sample/word_count/Input2.txt")
    input3 = os.path.join(os.path.dirname(__file__),
                          "sample/word_count/Input3.txt")
    manager = WordCountManager(config, [input1, input2, input3])
    for i in range(len(manager.reducers)):
        port = manager.find_free_port()
        manager.start_process('reducer', i, port)
        manager.reducer_addresses.append(f"[::]:{port}]")
    for i in range(len(manager.mappers)):
        port = manager.find_free_port()
        manager.start_process('mapper', i, port)
        manager.mapper_addresses.append(f"[::]:{port}")
    time.sleep(2)
    manager.run()
