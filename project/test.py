import os
import string
from multiprocessing import Pool
from collections import Counter
from collections import defaultdict

NUM_REDUCE_TASKS = 2


def mapper(file_path):
    with open(file_path) as f:
        word_count = Counter(f.read().split())
        print(word_count)

    # shard the word_count dictionary
    shard_size = max(1, len(word_count) // NUM_REDUCE_TASKS)
    shards = [dict(list(word_count.items())[i:i+shard_size])
              for i in range(0, len(word_count), shard_size)]

    # pass each shard to a reducer and get the results
    with Pool(NUM_REDUCE_TASKS) as p:
        p.daemon = False
        results = p.map(reducer, shards)

    # combine the results from all reducers
    combined_result = {}
    for result in results:
        for word, count in result.items():
            combined_result[word] = combined_result.get(word, 0) + count

    return combined_result


def reducer(shard):
    word_count = defaultdict(int)
    for shard_word_count in shard:
        for word, count in shard_word_count.items():
            word_count[word] += count

    return word_count


def combine_results(results):
    final_word_count = defaultdict(int)
    for result in results:
        for word, count in result.items():
            final_word_count[word] += count

    return final_word_count


def mapreduce(input_dir):
    input_files = [os.path.join(input_dir, filename) for filename in os.listdir(
        input_dir) if filename.endswith(".txt")]

    with Pool(processes=os.cpu_count()) as pool:
        pool.daemon = False
        map_results = pool.map(mapper, input_files)
        reduce_results = pool.map(reducer, map_results)

    final_word_count = combine_results(reduce_results)

    return final_word_count


if __name__ == "__main__":
    input_dir = os.path.join(os.path.dirname(__file__), "inputs")
    word_count = mapreduce(input_dir)
    print(word_count)
