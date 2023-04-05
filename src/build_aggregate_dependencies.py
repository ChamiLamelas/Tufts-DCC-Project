import pandas as pd
import os
import misc as c
from tqdm import tqdm
import time

CALLED_BY_ID = 'called_by'
CALLING_ID = 'calling'
TRACES_ID = 'traces'
MICROSERVICES_ID = 'microservices'




def collect_dependency_data(name):
    df = pd.read_csv(os.path.join(c.DATA_FOLDER, name))
    df = df[[c.UPSTREAM_ID, c.DOWNSTREAM_ID, c.TRACE_ID]]
    df = df[df[c.UPSTREAM_ID].notna()]
    df = df[df[c.DOWNSTREAM_ID].notna()]
    df = df[df[c.UPSTREAM_ID] != '(?)']
    df = df[df[c.DOWNSTREAM_ID] != '(?)']
    return df


def get_unique_microservices(dependency_df):
    return set(dependency_df[c.UPSTREAM_ID]).union(set(dependency_df[c.DOWNSTREAM_ID]))


def get_called_by(dependency_df):
    return dependency_df.groupby([c.DOWNSTREAM_ID])[c.UPSTREAM_ID].apply(set).to_dict()


def get_calling(dependency_df):
    return dependency_df.groupby([c.UPSTREAM_ID])[c.DOWNSTREAM_ID].apply(set).to_dict()


def get_traces_of_microservices(dependency_df):
    upstream_traces = dependency_df.groupby(
        [c.UPSTREAM_ID])[c.TRACE_ID].apply(set).to_dict()
    downstream_traces = dependency_df.groupby(
        [c.DOWNSTREAM_ID])[c.TRACE_ID].apply(set).to_dict()
    c.dict_value_union(upstream_traces, downstream_traces)
    return upstream_traces


def get_all_from_file(name):
    dependency_df = collect_dependency_data(name)
    return {
        CALLED_BY_ID: get_called_by(dependency_df),
        CALLING_ID: get_calling(dependency_df),
        TRACES_ID: get_traces_of_microservices(dependency_df),
        MICROSERVICES_ID: get_unique_microservices(dependency_df)
    }


def get_from_each_file():
    results = c.run_func_on_data_files(get_all_from_file)
    called_by, calling, traces, microservices = dict(), dict(), dict(), set()
    for result in tqdm(results, desc="Merging Graph, Trace, Microservice Data (single process)", total=len(results)):
        c.dict_value_union(called_by, result[CALLED_BY_ID])
        c.dict_value_union(calling, result[CALLING_ID])
        c.dict_value_union(traces, result[TRACES_ID])
        microservices = microservices.union(result[MICROSERVICES_ID])
    ti = time.time()
    microservices = list(microservices)
    traces = {k: len(v) for k, v in traces.items()}
    integer_map = {m: i for i, m in enumerate(microservices)}
    c.save_result_object(c.CALLED_BY_FILE, c.integerize(called_by, integer_map))
    c.save_result_object(c.CALLING_FILE, c.integerize(calling, integer_map))
    c.save_result_object(c.TRACES_FILE, c.integerize(traces, integer_map))
    c.save_result_object(c.MICROSERVICES_FILE, microservices)
    tf = time.time()
    c.debug(f"Integerizing and saving took {c.prettytime(tf - ti)}")


def main():
    get_from_each_file()


if __name__ == '__main__':
    main()
