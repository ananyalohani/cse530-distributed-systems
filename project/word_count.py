import os
import time
from typing import List
from collections import defaultdict

import map_reduce_pb2
from map_reduce import Manager, Mapper, Reducer


class WordCountMapper(Mapper):
    def Map(self, request, context):
        self.datastore = defaultdict(int)
        for filepath in self.filepaths:
            with open(filepath, "r") as input_file:
                for line in input_file:
                    for word in line.strip().split():
                        word = word.lower()
                        self.datastore[word] += 1
        self.num_reducers = request.num_reducers
        for key, value in self.datastore.items():
            self._map(key, value)
        return map_reduce_pb2.MapResponse(filepaths=list(self.shard_filepaths))

    def _map(self, key, value):
        shard_index = len(key) % self.num_reducers
        idx = shard_index + 1
        filepath = os.path.join(
            os.path.dirname(__file__),
            f"sample/word_count/map/intermediate{idx}.txt",
        )
        self.shard_filepaths.add(filepath)
        with open(filepath, "a") as f:
            f.write(f"{key} {value}\n")


class WordCountReducer(Reducer):
    def Reduce(self, request, context):
        filepaths = request.filepath
        with open(filepaths, "r") as f:
            for line in f:
                word, count = line.strip().split()
                self.datastore[word].append(int(count))
        for key, value in self.datastore.items():
            self._reduce(key, value)
        return map_reduce_pb2.ReduceResponse(status="OK")

    def _reduce(self, key: str, value: List[int]):
        count = 0
        for v in value:
            count = count + v
        with open(
            os.path.join(
                os.path.dirname(__file__),
                f"sample/word_count/reduce/output{self.id}.txt",
            ),
            "a",
        ) as f:
            f.write(f"{key} {count}\n")


if __name__ == "__main__":
    config = os.path.join(os.path.dirname(__file__), "sample/word_count/config.txt")
    input1 = os.path.join(os.path.dirname(__file__), "sample/word_count/Input1.txt")
    input2 = os.path.join(os.path.dirname(__file__), "sample/word_count/Input2.txt")
    input3 = os.path.join(os.path.dirname(__file__), "sample/word_count/Input3.txt")
    manager = Manager(
        "word_count",
        config,
        [input1, input2, input3, input1],
        WordCountMapper,
        WordCountReducer,
    )
    for i in range(len(manager.reducers)):
        port = manager.find_free_port()
        manager.start_process("reducer", i, port)
        manager.reducer_addresses.append(f"[::]:{port}]")
    for i in range(len(manager.mappers)):
        port = manager.find_free_port()
        manager.start_process("mapper", i, port)
        manager.mapper_addresses.append(f"[::]:{port}")
    time.sleep(2)
    manager.run()
