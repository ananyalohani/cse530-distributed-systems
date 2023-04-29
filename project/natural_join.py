import os
import time
import pandas
from functools import reduce
from collections import defaultdict

import map_reduce_pb2
from map_reduce import Manager, Mapper, Reducer


class NaturalJoinMapper(Mapper):
    def Map(self, request, context):
        common_col = self.args[0]
        self.datastore = defaultdict(list)
        self.num_reducers = request.num_reducers
        for filepath in self.filepaths:
            filename = os.path.basename(filepath)
            df = pandas.read_csv(filepath)
            common_idx = list(df.columns.values).index(common_col)
            other_idx = 1 - common_idx
            self._map(common_col, (filename, df.columns.values[other_idx]))
            for row, key in enumerate(df[common_col]):
                value = (filename, df.iloc[row][other_idx])
                self._map(key, value)
        return map_reduce_pb2.MapResponse(filepaths=self.shard_filepaths)

    def _map(self, key, value):
        shard_index = len(key) % self.num_reducers
        idx = shard_index + 1
        filepath = os.path.join(
            os.path.dirname(__file__),
            f"sample/natural_join/map/intermediate{idx}.txt",
        )
        self.shard_filepaths.add(filepath)
        with open(filepath, "a") as f:
            f.write(f"{key}, {value[0]} {value[1]}\n")


class NaturalJoinReducer(Reducer):
    def Reduce(self, request, context):
        self.datastore = defaultdict(list)
        with open(request.filepath, "r") as f:
            for line in f:
                line_st = line.strip()
                key_vals = line_st.split(", ")
                key = key_vals[0]
                value = key_vals[1].split()
                if "_table1" in value[0]:
                    value[0] = "T1"
                else:
                    value[0] = "T2"
                self.datastore[key].append(value)
        for key, value in self.datastore.items():
            self._reduce(key, value)
        return map_reduce_pb2.ReduceResponse(status="OK")

    def _reduce(self, key, value):
        if len(value) != 2:
            return
        row = [key, "", ""]
        for v in value:
            if v[0] == "T1":
                row[1] = v[1]
            else:
                row[2] = v[1]
        with open(
            os.path.join(
                os.path.dirname(__file__),
                f"sample/natural_join/reduce/output.txt",
            ),
            "a",
        ) as f:
            f.write(f"{row[0]}, {row[1]}, {row[2]}\n")


if __name__ == "__main__":
    operation = "natural_join"
    config = os.path.join(os.path.dirname(__file__), f"sample/{operation}/config.txt")
    input1 = os.path.join(
        os.path.dirname(__file__), f"sample/{operation}/input1_table1.txt"
    )
    input2 = os.path.join(
        os.path.dirname(__file__), f"sample/{operation}/input2_table1.txt"
    )
    input3 = os.path.join(
        os.path.dirname(__file__), f"sample/{operation}/input1_table2.txt"
    )
    input4 = os.path.join(
        os.path.dirname(__file__), f"sample/{operation}/input2_table2.txt"
    )

    inputs = [input1, input3]
    dfs = []
    for _input in inputs:
        df = pandas.read_csv(_input)
        if len(df.columns.values) != 2:
            raise ("Error: Tables must have two columns!")
        dfs.append(df)
    common_cols = list(reduce(lambda x, y: set(x.columns).intersection(y.columns), dfs))
    if len(common_cols) != 1:
        raise ("Error: Tables must have only one common column!")

    manager = Manager(
        "natural_join",
        config,
        inputs,
        NaturalJoinMapper,
        NaturalJoinReducer,
        [[input1, input3], [input2, input4]],
        common_cols[0],
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
