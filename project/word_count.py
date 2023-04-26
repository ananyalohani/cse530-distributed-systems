import json
import os
import time
from collections import defaultdict

import map_reduce_pb2
from map_reduce import Manager, Mapper, Reducer, Partitioner


class WordCountMapper(Mapper):
    def Map(self, request, context):
        self.datastore = defaultdict(int)
        for filepath in self.filepaths:
            with open(filepath, "r") as input_file:
                for line in input_file:
                    for word in line.strip().split():
                        word = word.lower()
                        self.datastore[word] += 1
        num_reducers = request.num_reducers
        partitioner = Partitioner(num_reducers)
        shards = [defaultdict(int) for _ in range(num_reducers)]
        for word, count in self.datastore.items():
            shard_index = partitioner.partition(word)
            shards[shard_index][word] += count
        return map_reduce_pb2.MapResponse(shards=json.dumps(shards))

class WordCountReducer(Reducer):
    def Reduce(self, request, context):
        shards = json.loads(request.shards)
        for datastore in shards:
            datastore = defaultdict(int, datastore)
            for word, count in datastore.items():
                self.datastore[word] += count
        with open(os.path.join(
            os.path.dirname(__file__),
            f"sample/word_count/wc_output{self.id}.txt"
        ), "w") as f:
            for key, value in self.datastore.items():
                f.write(f"{key} {value}\n")
        return map_reduce_pb2.ReduceResponse(
            datastore=json.dumps(self.datastore),
        )


class WordCountManager(Manager):
    def local_reduce(self, datastore):
        store = defaultdict(int, datastore)
        for key, value in store.items():
            self.datastore[key] += value


if __name__ == "__main__":
    config = os.path.join(os.path.dirname(__file__),
                          "sample/word_count/config.txt")
    input1 = os.path.join(os.path.dirname(__file__),
                          "sample/word_count/Input1.txt")
    input2 = os.path.join(os.path.dirname(__file__),
                          "sample/word_count/Input2.txt")
    input3 = os.path.join(os.path.dirname(__file__),
                          "sample/word_count/Input3.txt")
    manager = WordCountManager(
        config, [input1, input2, input3], WordCountMapper, WordCountReducer)
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
