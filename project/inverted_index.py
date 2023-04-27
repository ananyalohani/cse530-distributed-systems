import os
import time
from collections import defaultdict

import map_reduce_pb2
from map_reduce import Manager, Mapper, Reducer


class InvertedIndexMapper(Mapper):
    def Map(self, request, context):
        self.datastore = defaultdict(set)
        for filepath in self.filepaths:
            basepath = os.path.basename(filepath)
            document_id = basepath.split(".")[0].split("Input")[1]
            with open(filepath, "r") as input_file:
                for line in input_file:
                    for word in line.strip().split(" "):
                        word = word.lower()
                        if word not in self.datastore:
                            self.datastore[word] = set(document_id)
                        else:
                            self.datastore[word].add(document_id)

        self.num_reducers = request.num_reducers
        for key, value in self.datastore.items():
            self._map(key, value)
        return map_reduce_pb2.MapResponse(filepaths=list(self.shard_filepaths))

    def _map(self, key, value):
        shard_index = len(key) % self.num_reducers
        idx = shard_index + 1
        filepath = os.path.join(
            os.path.dirname(__file__),
            f"sample/inverted_index/map/intermediate{idx}.txt",
        )
        self.shard_filepaths.add(filepath)
        with open(filepath, "a") as f:
            f.write(f"{key} {','.join(value)}\n")


class InvertedIndexReducer(Reducer):
    def Reduce(self, request, context):
        filepaths = request.filepath
        with open(filepaths, "r") as f:
            for line in f:
                word, indices = line.strip().split(" ")
                if word in self.datastore:
                    self.datastore[word] += indices.split(",")
                else:
                    self.datastore[word] = indices.split(",")
        inverted_index = defaultdict(set)
        for key, value in self.datastore.items():
            for v in value:
                inverted_index[key].add(v)
        for key, value in inverted_index.items():
            self._reduce(key, value)
        return map_reduce_pb2.ReduceResponse(status="OK")

    def _reduce(self, key: str, value: set):
        with open(
            os.path.join(
                os.path.dirname(__file__),
                f"sample/inverted_index/reduce/output{self.id}.txt",
            ),
            "a",
        ) as f:
            f.write(f"{key} {','.join(value)}\n")


if __name__ == "__main__":
    config = os.path.join(os.path.dirname(__file__), "sample/inverted_index/config.txt")
    input1 = os.path.join(os.path.dirname(__file__), "sample/inverted_index/Input1.txt")
    input2 = os.path.join(os.path.dirname(__file__), "sample/inverted_index/Input2.txt")
    input3 = os.path.join(os.path.dirname(__file__), "sample/inverted_index/Input3.txt")
    manager = Manager(
        "inverted_index",
        config,
        [input1, input2, input3],
        InvertedIndexMapper,
        InvertedIndexReducer,
    )
    for i in range(len(manager.reducers)):
        port = manager.find_free_port()
        manager.start_process("reducer", i, port)
        manager.reducer_addresses.append(f"[::]:{port}")
    for i in range(len(manager.mappers)):
        port = manager.find_free_port()
        manager.start_process("mapper", i, port)
        manager.mapper_addresses.append(f"[::]:{port}")
    time.sleep(2)
    manager.run()
