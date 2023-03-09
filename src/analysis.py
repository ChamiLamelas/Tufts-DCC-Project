import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
from scipy.stats import pearsonr
from collections import defaultdict
import os
import csv


UPSTREAM_ID = 'um'
DOWNSTREAM_ID = 'dm'
TRACE_ID = 'traceid'
RPC_ID = 'rpcid'
ROOT_RPCID = ''

NUM_FILES = 4


def get_parent(rpcid):
    # Gets parent RPC IDs
    return ROOT_RPCID if '.' not in rpcid else rpcid[:rpcid.rindex('.')]


def get_csvs():
    # Get CSVs in directory
    return [os.path.join("..", "data", "MSCallGraph_" + str(i) + ".csv") for i in range(NUM_FILES)]


def update(src, more):
    # Takes 2 Any -> set dicts and unions their values
    # Assumes any src[k] ok for any k in more
    for k, v in more.items():
        src[k] = src[k].union(v)


def listify(d):
    # Converts Any -> Iterable dict to Any -> list
    return {k: list(v) for k, v in d.items()}


def integerize(d, index_map):
    # Maps Any -> list[Any] dict -> int -> list[int] dict using index_map
    # to map keys and elements in values to ints
    return {d[index_map[k]]: [index_map[e] for e in v] for k, v in d.items()}


def get_trace_data(csv_path):
    # Gets trace data from a file
    cols = list()
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        next(iter(reader))
        cols = [[row[1], row[3]] for row in reader]
    return cols


def get_graph(trace):
    # Constructs call graph (tree) from trace
    graph = defaultdict(list)
    for rpcid in trace:
        graph[get_parent(rpcid)].append(rpcid)
    return dict(graph)


# Sliding window won't work
# class Traces:
#     def __init__(self):
#         self.csvs = get_csvs()
#         self.csv = 0
#         self.row = 0
#         self.trace_data = None

#     def __read_csv(self, csv, traceid, trace):
#         # Reads trace csv
#         if self.trace_data is None:
#             self.trace_data = get_trace_data(csv)
#         if traceid is None:
#             traceid = self.trace_data[self.row][0]
#             self.row += 1
#         while self.row < len(self.trace_data) and self.trace_data[self.row][0] == traceid:
#             self.row += 1
#             trace.append(self.trace_data[self.row][1])
#         if self.row == len(self.trace_data):
#             self.trace_data = None
#         return traceid

#     def next_trace(self):
#         trace = list()
#         traceid = self.__read_csv(self.csvs[self.csv], None, trace)
#         if self.trace_data is not None:
#             return traceid, trace
#         self.csv += 1
#         self.row = 0
#         if self.csv == len(self.csvs):
#             return traceid, trace
#         self.__read_csv(self.csvs[self.csv], traceid, trace)
#         return traceid, trace


class Graphs:
    def __init__(self):
        csvs = get_csvs()

        # Will be built up over files
        self.called_by = defaultdict(set)
        self.calling = defaultdict(set)
        all_traces = defaultdict(set)

        for call_graph_csv in csvs:
            # Load in traces file and then remove any rows with unidentifiable microservices
            df = pd.read_csv(call_graph_csv)
            df = df[[UPSTREAM_ID, DOWNSTREAM_ID, TRACE_ID]]
            df = df[df[UPSTREAM_ID].notna()]
            df = df[df[DOWNSTREAM_ID].notna()]
            df = df[df[UPSTREAM_ID] != "(?)"]
            df = df[df[DOWNSTREAM_ID] != "(?)"]

            # Collects set of UMs for each DM - update what we have for all files - update( )
            # is doing a set union, so this will keep our UMs seen for each DM unique
            called_by = df.groupby([DOWNSTREAM_ID])[
                UPSTREAM_ID].apply(set).to_dict()
            update(self.called_by, called_by)

            # Collects set of DMs for each UM - update what we have for all files in same way
            calling = df.groupby([UPSTREAM_ID])[
                DOWNSTREAM_ID].apply(set).to_dict()
            update(self.calling, calling)

            # for each UM, collect all traces they appear in - all_traces maps a microservice
            # ID to all traces it appears in (as either upstream or downstream)
            upstream_traces = df.groupby([UPSTREAM_ID])[
                TRACE_ID].apply(set).to_dict()
            update(all_traces, upstream_traces)

            # for each DM, collect all traces they appear in - since we're doing update( ) i.e.
            # a set union - we keep traces unique based on microservice
            downstream_traces = df.groupby([DOWNSTREAM_ID])[
                TRACE_ID].apply(set).to_dict()
            update(all_traces, downstream_traces)

        # change mapping to be to lists instead of sets
        self.called_by = listify(self.called_by)
        self.calling = listify(self.calling)

        # Identify unique microservices from keys of all traces (collects both UMs and DMs already
        # could also have done union of keys in called_by and calling)
        self.microservices = list(all_traces)

        # Derive trace frequencies from unique traces per microservice
        self.trace_freq = {k: len(v) for k, v in all_traces.items()}

        # Convert everything to indices after building enumeration index map
        index_map = {k: i for i, k in enumerate(self.microservices)}
        self.called_by_iz = integerize(self.called_by, index_map)
        self.calling_iz = integerize(self.calling, index_map)
        self.trace_freq_iz = integerize(self.trace_freq, index_map)


def plot_histogram(graphs, names, paths, nbins=50):
    for p, n, g in zip(paths, names, [graphs.called_by_iz, graphs.calling_iz]):
        plt.figure()
        d = [len(v) for v in g.values()]
        plt.hist(d, bins=nbins)
        plt.suptitle(n + " Distribution")
        plt.title(f"Minimum {n} Count={min(d)}, Maximum={max(d)}")
        plt.xlabel(n)
        plt.ylabel("Frequency")
        plt.grid()
        plt.savefig(p)


def calculate_sparsity_ratio(graphs, path):
    total_edges = len(graphs.microservices) ** 2
    num_edges = sum(len(v) for v in graphs.called_by_iz.values())
    Path(path).write_text(f"{num_edges/total_edges:.4f}\n")


def calculate_called_by1(graphs, path):
    output = [k for k, v in graphs.called_by.items() if len(v) == 1]
    print(f"{len(output)} microservices are called by only 1 other microservice.")
    Path(path).write_text("\n".join(output) + "\n")


def __helper_bfs(unexplored, undirected):
    start = next(iter(unexplored))
    queue = [start]
    unexplored.remove(start)
    size = 0
    while len(queue) > 0:
        x = queue.pop(0)
        for n in undirected[x]:
            if n in unexplored:
                unexplored.remove(n)
                queue.append(n)
        size += 1
    return size


def calculate_connected_components(graphs, path):
    undirected = graphs.called_by_iz.copy()
    for k, v in graphs.calling_iz.items():
        undirected[k] = list(set(undirected[k]).union(
            set(v))) if k in undirected else v.copy()
    unexplored = set(undirected)
    connected_sizes = list()
    while len(unexplored) > 0:
        connected_sizes.append(__helper_bfs(unexplored, undirected))
    Path(path).write_text(str(sorted(connected_sizes, reverse=True)) + "\n")


def calculate_correlation(graphs, paths):
    x1 = list()
    x2 = list()
    y = list()
    for k, v in graphs.called_by_iz.items():
        x1.append(v)
        x2.append(graphs.calling_iz[k])
        y.append(graphs.trace_freq_by_iz[k])
    plt.figure()
    plt.plot(x1, y)
    plt.suptitle("Correlation between Called-By and Trace Frequency")
    plt.title(f"r = {pearsonr(x1, y)[0]:.4f}")
    plt.grid()
    plt.savefig(paths[0])
    plt.figure()
    plt.plot(x2, y)
    plt.suptitle("Correlation between Calling and Trace Frequency")
    plt.title(f"r = {pearsonr(x2, y)[0]:.4f}")
    plt.grid()
    plt.savefig(paths[1])
