import os
from pathlib import Path
import sys
import multiprocessing as mp
import time
import pickle

DATA_FOLDER = os.path.join('..', 'data')
RESULT_FOLDER = os.path.join('..', 'results')
TRACE_DATA_FOLDER = os.path.join(DATA_FOLDER, 'traces')

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
TRACE_FILES_FILE = os.path.join(TRACES, 'files.pkl')

TRACE_COLUMNS = [TRACE_ID, UPSTREAM_ID,
                 DOWNSTREAM_ID, RPC_ID, RPCTYPE_ID, TIMESTAMP_ID]

MISSING_MICROSERVICE = -1

CONCURRENCY_FILE = os.path.join(TRACES, 'concurrency.pkl')
DEPTH_FILE = os.path.join(TRACES, 'depths.pkl')

NICE_TRACES_FILE = os.path.join(DATA_FOLDER, "nice_traces.pkl")
NON_UNIQ_TRACES_FILE = os.path.join(DATA_FOLDER, "not_uniq_rpcid_traces.pkl")
MISSING_MS_TRACES_FILE = os.path.join(DATA_FOLDER, "missing_one_traces.pkl")
MISSING_BOTH_TRACES_FILE = os.path.join(DATA_FOLDER, "missing_both_traces.pkl")

__UPSTREAM_ID_IDX = TRACE_COLUMNS.index(UPSTREAM_ID)
__DOWNSTREAM_ID_IDX = TRACE_COLUMNS.index(DOWNSTREAM_ID)
__RPC_ID_IDX = TRACE_COLUMNS.index(RPC_ID)
__RPCTYPE_ID_IDX = TRACE_COLUMNS.index(RPCTYPE_ID)
__TIMESTAMP_ID_IDX = TRACE_COLUMNS.index(TIMESTAMP_ID)


def um(row):
    return row[__UPSTREAM_ID_IDX - 1]


def dm(row):
    return row[__DOWNSTREAM_ID_IDX - 1]


def rpc(row):
    return row[__RPC_ID_IDX - 1]


def rpctype(row):
    return row[__RPCTYPE_ID_IDX - 1]


def timestamp(row):
    return row[__TIMESTAMP_ID_IDX - 1]


def set_um(row, um):
    row[__UPSTREAM_ID_IDX - 1] = um


def set_dm(row, dm):
    row[__DOWNSTREAM_ID_IDX - 1] = dm


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


def dict_value_extend(dict1, dict2):
    for k, v in dict2.items():
        if k not in dict1:
            dict1[k] = v
        else:
            dict1[k].extend(v)


def read_object(path):
    with open(path, 'rb') as handle:
        return pickle.load(handle)


def read_result_object(path):
    return read_object(os.path.join(RESULT_FOLDER, path))


def save_object(path, obj):
    prep_path(path)
    with open(path, 'wb') as handle:
        pickle.dump(obj, handle, protocol=pickle.HIGHEST_PROTOCOL)


def save_result_object(path, obj):
    save_object(os.path.join(RESULT_FOLDER, path), obj)


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
        save_result_object(f + ".tmp", func(f, *extra))
    return target


def run_func_on_data_files(func, *extra_args, return_results=True):
    files = get_csvs()[:2]
    ti = time.time()
    processes = [None] * len(files)
    for i, f in enumerate(files):
        if return_results:
            processes[i] = mp.Process(
                target=make_target(func), args=(f,) + extra_args)
        else:
            processes[i] = mp.Process(
                target=func, args=(f,) + extra_args)
        processes[i].start()
    for p in processes:
        p.join()
    results = None
    if return_results:
        results = list()
        for f in files:
            results.append(read_result_object(f + ".tmp"))
            os.remove(os.path.join(RESULT_FOLDER, f + ".tmp"))
    tf = time.time()
    debug(
        f"Ran {func.__name__} on {len(files)} processes in {prettytime(tf - ti)}")
    return results


def get_cmdline_arg(func=None):
    assert len(sys.argv) == 2
    return sys.argv[1] if func is None else func(sys.argv[1])


def dict_val_filter(func, d):
    return {k: v for k, v in d.items() if func(v)}


def dict_get_n(d, n):
    return {k: v for k, v in list(d.items())[:n]}


def dict_get_1(d):
    d = dict_get_n(d, 1)
    return list(d.keys())[0], list(d.values())[0]


def count_on_condition(ls, condition):
    return sum(condition(e) for e in ls)


def has_condition_match(ls, condition):
    for e in ls:
        if condition(e):
            return True
    return False


def make_blank_row(rpcid):
    return [-2, -3, rpcid, 'sub', 0]


def make_hierarchy(trace):
    return sorted(trace, key=lambda r: int(rpc(r).replace('.', '')))


def nice_display(trace):
    print('\n' + "\n".join(str(row) for row in trace) + '\n')

