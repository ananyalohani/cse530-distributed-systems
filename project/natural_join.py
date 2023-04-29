import os, time

from map_reduce import Manager, Mapper, Reducer
import map_reduce_pb2


class NaturalJoinMapper(Mapper):
    def Map(self, request, context):
        pass

    def _map(self, key, value):
        pass


class NaturalJoinReducer(Reducer):
    def Reduce(self, request, context):
        pass

    def _reduce(self, key, value):
        pass


if __name__ == "__main__":
    config = os.path.join(os.path.dirname(__file__), "sample/natural_join/config.txt")
    input1_table1 = os.path.join(
        os.path.dirname(__file__), "sample/natural_join/input1_table1.txt"
    )
    input1_table2 = os.path.join(
        os.path.dirname(__file__), "sample/natural_join/input1_table2.txt"
    )
    input2_table1 = os.path.join(
        os.path.dirname(__file__), "sample/natural_join/input2_table1.txt"
    )
    input2_table2 = os.path.join(
        os.path.dirname(__file__), "sample/natural_join/input2_table2.txt"
    )
    manager = Manager(
        "word_count",
        config,
        [],
        NaturalJoinMapper,
        NaturalJoinReducer,
        [[input1_table1, input1_table2], [input2_table1, input2_table2]],
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
