import csv
import itertools
import os
import time

import map_reduce_pb2
import pandas
from map_reduce import Manager, Mapper, Reducer


class NaturalJoinMapper(Mapper):
    def Map(self, request, context):
        self.num_reducers = request.num_reducers
        file_dfs = []
        tables = []
        for filepath in self.filepaths:
            df = pandas.read_csv(filepath, engine="python", sep=", ")
            file_dfs.append(df)

            # * Assumption: File name is of the form input1_table1.txt
            filename = os.path.basename(filepath).split(".")[0]
            table = "T" + filename.split("_")[1][-1]
            tables.append(table)

        # * Assumption: Only one common column
        common_col = list(set.intersection(*[set(df.columns) for df in file_dfs]))[0]
        for i, df in enumerate(file_dfs):
            columns = df.columns.values
            for j, row in df.iterrows():
                key = (row[common_col], common_col)
                for col in columns:
                    if col != common_col:
                        value = (tables[i], row[col], col)
                        self._map(key, value)
        return map_reduce_pb2.MapResponse(filepaths=list(self.shard_filepaths))

    def _map(self, key, value):
        shard_index = 0
        if type(key[0]) == str:
            shard_index = len(key[0]) % self.num_reducers
        elif type(key[0]) == int:
            shard_index = key[0] % self.num_reducers
        idx = shard_index + 1
        filepath = os.path.join(
            os.path.dirname(__file__),
            f"sample/natural_join/map/intermediate{idx}.txt",
        )
        self.shard_filepaths.add(filepath)
        with open(filepath, "a") as f:
            f.write(f"{key} {value}\n")


class NaturalJoinReducer(Reducer):
    def Reduce(self, request, context):
        keys = []
        values = []
        cols = set()
        tables = set()

        # Read from intermediate file
        file_content = self.get_intermediate_file(
            mapper_address=request.mapper_address, filepath=request.filepath
        )
        for line in file_content.split("\n"):
            key, value = line.strip().split(") (")
            key = tuple(k.strip("'") for k in key.strip("()").split(", "))
            value = tuple(v.strip("'") for v in value.strip("()").split(", "))
            cols.add(value[2])
            tables.add(value[0])
            if key not in keys:
                keys.append(key)
                values.append([value])
            else:
                values[keys.index(key)].append(value)

        # Write csv header to output file
        cols = list(sorted(cols))
        cols.insert(0, keys[0][1])
        with open(
            os.path.join(
                os.path.dirname(__file__),
                f"sample/natural_join/reduce/output{self.id}.txt",
            ),
            "a",
        ) as f:
            writer = csv.writer(f)
            writer.writerow(cols)

        # Get keys with all tables are present in the input
        new_keys = []
        new_values = []
        for key, value in zip(keys, values):
            vtables = set()
            for v in value:
                vtables.add(v[0])
            if len(tables) == len(vtables):
                new_keys.append(key)
                new_values.append(value)

        # Group values for each key
        table_values = [{} for _ in range(len(new_keys))]
        for key, value in zip(new_keys, new_values):
            idx = new_keys.index(key)
            table_values[idx][key[1]] = key[0]
            for v in value:
                if v[2] not in table_values[idx]:
                    table_values[idx][v[2]] = set()
                table_values[idx][v[2]].add(v)
            for v in value:
                table_values[idx][v[2]] = list(table_values[idx][v[2]])

        # Get cartesian product of values for each key
        final_rows = [{"key": key, "values": []} for key in new_keys]
        for i, row in enumerate(table_values):
            keys = list(row.keys())
            keys.remove(new_keys[0][1])
            prod = list(itertools.product(*[row[k] for k in keys]))
            final_rows[i]["values"].extend(prod)

        # Reduce
        for row in final_rows:
            key = row["key"]
            for value in row["values"]:
                self._reduce(key, value)

        return map_reduce_pb2.ReduceResponse(status="OK")

    def _reduce(self, key, value):
        key = key[0]
        values = []
        for v in value:
            values.append(v[1])
        with open(
            os.path.join(
                os.path.dirname(__file__),
                f"sample/natural_join/reduce/output{self.id}.txt",
            ),
            "a",
        ) as f:
            writer = csv.writer(f)
            writer.writerow([key, *values])


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
    # input1_table1 = os.path.join(
    #     os.path.dirname(__file__), "sample/natural_join/inputs/input1_table1.txt"
    # )
    # input1_table2 = os.path.join(
    #     os.path.dirname(__file__), "sample/natural_join/inputs/input1_table2.txt"
    # )
    # input1_table3 = os.path.join(
    #     os.path.dirname(__file__), "sample/natural_join/inputs/input1_table3.txt"
    # )
    # input2_table1 = os.path.join(
    #     os.path.dirname(__file__), "sample/natural_join/inputs/input2_table1.txt"
    # )
    # input2_table2 = os.path.join(
    #     os.path.dirname(__file__), "sample/natural_join/inputs/input2_table2.txt"
    # )
    # input2_table3 = os.path.join(
    #     os.path.dirname(__file__), "sample/natural_join/inputs/input2_table3.txt"
    # )
    manager = Manager(
        "natural_join",
        config,
        [],
        NaturalJoinMapper,
        NaturalJoinReducer,
        [
            [input1_table1, input1_table2],
            [input2_table1, input2_table2],
        ],
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
