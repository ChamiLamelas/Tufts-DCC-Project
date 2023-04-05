# https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2021

import os
from pathlib import Path
import sys
import multiprocessing as mp
import time
import pickle

DATA_FOLDER = os.path.join('..', 'data')
RESULT_FOLDER = os.path.join('..', 'results')

UPSTREAM_ID = 'um'
DOWNSTREAM_ID = 'dm'
TRACE_ID = 'traceid'
TIMESTAMP_ID = 'timestamp'
RPC_ID = 'rpcid'
RPCTYPE_ID = 'rpctype'

AGGREGATE_DEPENDENCY = 'aggregate_dependency'
TRACES = 'traces'
ERRORS = 'errors'

CALLED_BY_FILE = os.path.join(AGGREGATE_DEPENDENCY, 'called_by.pkl')
CALLING_FILE = os.path.join(AGGREGATE_DEPENDENCY, 'calling.pkl')
TRACES_FILE = os.path.join(AGGREGATE_DEPENDENCY, 'traces.pkl')
MICROSERVICES_FILE = os.path.join(AGGREGATE_DEPENDENCY, 'microservices.pkl')

TRACE_IDS_FILE = os.path.join(TRACES, 'ids.pkl')
TRACE_RPCS_FILE = os.path.join(TRACES, 'rpcs.pkl')


def integerize(d, index_map):
    # Given called by / calling maps dict[str, list[str]] -> dict[int, list[int]]
    # And frequency dict[str, int] -> dict[int, int] using index_map
    if isinstance(next(iter(d.values())), int):
        return {index_map[k]: v for k, v in d.items()}
    return {index_map[k]: [index_map[e] for e in v] for k, v in d.items()}


def dict_value_union(dict1, dict2):
    for k, v in dict2.items():
        if k not in dict1:
            dict1[k] = v
        else:
            dict1[k] = dict1[k].union(v)


def read_object(path):
    with open(os.path.join(RESULT_FOLDER, path), 'rb') as handle:
        return pickle.load(handle)


def save_object(path, obj):
    prep_path(os.path.join(RESULT_FOLDER, path))
    with open(os.path.join(RESULT_FOLDER, path), 'wb') as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)


def prettytime(secs):
    mins, secs = divmod(secs, 60)
    return (f"{mins}m " if mins > 0 else "") + f"{secs:.2f}s"


def debug(str):
    print(str, file=sys.stderr)


def get_csvs():
    return sorted([e.name for e in os.scandir(DATA_FOLDER) if e.is_file() and e.name.endswith('.csv')], key=lambda e: int(e[e.rindex("_")+1:-4]))


def prep_path(path):
    Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)


def write_text(path, text):
    prep_path(path)
    Path(path).write_text(text)


def make_target(func):
    def target(f, *extra):
        save_object(f + ".tmp", func(f, *extra))
    return target


def run_func_on_data_files(func, extra_args=tuple()):
    files = get_csvs()
    ti = time.time()
    processes = [None] * len(files)
    for i, f in enumerate(files):
        processes[i] = mp.Process(
            target=make_target(func), args=(f,) + extra_args)
        processes[i].start()
    for p in processes:
        p.join()
    results = list()
    for f in files:
        results.append(read_object(f + ".tmp"))
        os.remove(os.path.join(RESULT_FOLDER, f + ".tmp"))
    tf = time.time()
    debug(
        f"Ran {func.__name__} on {len(files)} processes in {prettytime(tf - ti)}")
    return results
