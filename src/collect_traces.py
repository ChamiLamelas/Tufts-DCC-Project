import pandas as pd
from collections import defaultdict
import os
import misc as c
from tqdm import tqdm
import time

ROOT_RPCID = ''


def get_trace_data(path, integer_map):
    TRACE_COLUMNS = [c.TRACE_ID, c.UPSTREAM_ID, c.DOWNSTREAM_ID,
                     c.RPC_ID, c.RPCTYPE_ID, c.TIMESTAMP_ID]
    df = pd.read_csv(os.path.join(c.DATA_FOLDER, path))
    df = df[TRACE_COLUMNS]
    df[c.UPSTREAM_ID] = df[c.UPSTREAM_ID].apply(
        lambda x: integer_map[x]).astype('int')
    df[c.DOWNSTREAM_ID] = df[c.DOWNSTREAM_ID].apply(
        lambda x: integer_map[x]).astype('int')
    return df.groupby([c.TRACE_ID])[TRACE_COLUMNS].to_dict()


def get_from_each_file(microservices):
    microservice_integer_map = {m: i for i, m in enumerate(microservices)}
    results = c.run_func_on_data_files(
        get_trace_data, microservice_integer_map)
    traces = dict()
    for result in tqdm(results, desc="Merging Trace RPCs (single process)", total=len(results)):
        c.dict_value_union(traces, result)
    ti = time.time()
    c.save_object(c.TRACE_IDS_FILE, [t for t in traces])
    c.save_object(c.TRACE_RPCS_FILE, list(traces.values()))
    tf = time.time()
    c.debug(f"Saving took {c.prettytime(tf - ti)}")


def get_parent(rpcid):
    # Gets parent RPC IDs
    return ROOT_RPCID if '.' not in rpcid else rpcid[:rpcid.rindex('.')]


def get_graph(trace):
    # Constructs call graph (tree) from trace
    graph = defaultdict(list)
    for rpcid in trace:
        graph[get_parent(rpcid)].append(rpcid)
    return dict(graph)
