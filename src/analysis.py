import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import logging
from pathlib import Path
import os

UPSTREAM_ID = 'um'
DOWNSTREAM_ID = 'dm'
LOGS = os.path.join("..", "logs")

# Upstream service calls downstream service


class Graphs:
    def __init__(self, call_graph_csv):
        logger = Graphs.__make_logger()
        logger.debug("Building Graphs")
        df = pd.read_csv(call_graph_csv)
        rows = len(df)
        logger.debug(f"Read in {rows} row table")
        df = df[[UPSTREAM_ID, DOWNSTREAM_ID]]
        df = df[df[UPSTREAM_ID].notna()]
        df = df[df[DOWNSTREAM_ID].notna()]
        df = df[df[UPSTREAM_ID] != "(?)"]
        df = df[df[DOWNSTREAM_ID] != "(?)"]
        postrows = len(df)
        logger.debug(f"{rows-postrows}/{rows} rows removed for nan/(?) microservices")
        self.called_by = df.groupby([DOWNSTREAM_ID])[
            UPSTREAM_ID].unique().to_dict()
        self.calling = df.groupby([UPSTREAM_ID])[
            DOWNSTREAM_ID].unique().to_dict()
        self.microservices = list(
            self.called_by) + [e for e in self.calling if e not in self.called_by]        
        logger.debug(f"Identified {len(self.microservices)} unique microservices")
        logger.debug(f"Identified strange microservices: {', '.join(ms for ms in self.microservices if len(ms) < 10)}")
        index_map = {k: i for i, k in enumerate(self.microservices)}
        self.called_by_lz = self.__listize(self.called_by, index_map)
        self.calling_lz = self.__listize(self.calling, index_map)

    @staticmethod
    def __make_logger():
        Path(LOGS).mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger('Graphs')
        logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler(os.path.join(LOGS, "Graphs.log"))
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        return logger

    def __listize(self, graph, index_map):
        idz = [[] for _ in self.microservices]
        for k, v in graph.items():
            idz[index_map[k]].extend(index_map[e] for e in v)
        return idz



def __get_degrees(graph_lz):
    return [len(e) for e in graph_lz]


def __make_degree_matrix(graph_lz, num_ms):
    matrix = np.zeros((num_ms, num_ms))
    for i, d in enumerate(__get_degrees(graph_lz)):
        matrix[i, i] = d
    return matrix


def plot_degree_matrix(graphs, paths):
    for p, g in zip(paths, [graphs.called_by_lz, graphs.calling_lz]):
        plt.figure()
        plt.imshow(__make_degree_matrix(g, len(graphs.microservices)),
                   cmap='Reds', interpolation='nearest')
        plt.savefig(p)


def plot_histogram(graphs, names, paths, nbins=50):
    for p, n, g in zip(paths, names, [graphs.called_by_lz, graphs.calling_lz]):
        plt.figure()
        d = __get_degrees(g)
        plt.hist(d, bins=nbins)
        plt.suptitle(n + " Distribution")
        plt.title(f"min={min(d)}, max={max(d)}")
        plt.xlabel(n)
        plt.ylabel("Count")
        plt.grid()
        plt.savefig(p)

# calculate the ratio of what would be 1s/(0s + 1s) in an adjacency matrix as the sparsity ratio
# another idea - draw graph without edges and node size is how much graph is called by / calling
# report microservices that are called by only 1 other microservice - those could potentially be combined

