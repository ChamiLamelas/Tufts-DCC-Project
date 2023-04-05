import pandas as pd
import os
import misc as c
from collections import defaultdict


def get_trace_data(path, ms_integer_map, trace_integer_map, to_remove):
    df = pd.read_csv(os.path.join(c.DATA_FOLDER, path))
    df = df[c.TRACE_COLUMNS]
    df[c.TRACE_ID] = df[c.TRACE_ID].apply(
        lambda x: trace_integer_map[x]).astype('int')
    df = df[~df[c.TRACE_ID].isin(to_remove)]
    df[c.UPSTREAM_ID] = df[c.UPSTREAM_ID].apply(
        lambda x: ms_integer_map[x]).astype('int')
    df[c.DOWNSTREAM_ID] = df[c.DOWNSTREAM_ID].apply(
        lambda x: ms_integer_map[x]).astype('int')
    as_dict = df.groupby([c.TRACE_ID])[c.TRACE_COLUMNS[1:]].apply(
        lambda x: x.values.tolist()).to_dict()
    return as_dict


def collect_prerequisites():
    microservices = c.read_result_object(c.MICROSERVICES_FILE)
    traces = c.read_result_object(c.TRACE_IDS_FILE)
    trace_files = c.read_result_object(c.TRACE_FILES_FILE)
    ms_integer_map = defaultdict(lambda: c.MISSING_MICROSERVICE)
    ms_integer_map.update({m: i for i, m in enumerate(microservices)})
    trace_integer_map = {t: i for i, t in enumerate(traces)}
    to_remove = {i for i, files in enumerate(trace_files) if len(files) > 1}
    return ms_integer_map, trace_integer_map, to_remove
