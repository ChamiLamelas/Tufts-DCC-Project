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
        logger.debug(
            f"{rows-postrows}/{rows} rows removed for nan/(?) microservices")
        self.called_by = df.groupby([DOWNSTREAM_ID])[
            UPSTREAM_ID].unique().to_dict()
        self.calling = df.groupby([UPSTREAM_ID])[
            DOWNSTREAM_ID].unique().to_dict()
        self.microservices = list(
            self.called_by) + [e for e in self.calling if e not in self.called_by]
        logger.debug(
            f"Identified {len(self.microservices)} unique microservices")
        logger.debug(
            f"Identified strange microservices: {', '.join(ms for ms in self.microservices if len(ms) < 10)}")
        index_map = {k: i for i, k in enumerate(self.microservices)}
        self.called_by_iz = self.__integerize(self.called_by, index_map)
        self.calling_iz = self.__integerize(self.calling, index_map)

    @staticmethod
    def __make_logger():
        Path(LOGS).mkdir(parents=True, exist_ok=True)
        logger = logging.getLogger('Graphs')
        logger.setLevel(logging.DEBUG)
        handler = logging.FileHandler(os.path.join(LOGS, "Graphs.log"))
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        handler.setLevel(logging.DEBUG)
        logger.addHandler(handler)
        return logger

    def __integerize(self, graph, index_map):
        return {index_map[k]: [index_map[e] for e in v] for k, v in graph.items()}


def __make_degree_matrix(graph_iz, num_ms):
    matrix = np.zeros((num_ms, num_ms))
    for k, v in graph_iz.items():
        matrix[k, k] = len(v)
    return matrix


def plot_degree_matrix(graphs, paths):
    for p, g in zip(paths, [graphs.called_by_iz, graphs.calling_iz]):
        plt.figure()
        plt.imshow(__make_degree_matrix(g, len(graphs.microservices)),
                   cmap='Reds', interpolation='nearest')
        plt.savefig(p)


def plot_histogram(graphs, names, paths, nbins=50):
    for p, n, g in zip(paths, names, [graphs.called_by_iz, graphs.calling_iz]):
        plt.figure()
        d = [len(v) for v in g.values()]
        plt.hist(d, bins=nbins)
        plt.suptitle(n + " Distribution")
        plt.title(f"min={min(d)}, max={max(d)}")
        plt.xlabel(n)
        plt.ylabel("Count")
        plt.grid()
        plt.savefig(p)


def calculate_sparsity_ratio(graphs, path):
    total_edges = len(graphs.microservices) ** 2
    num_edges = sum(len(v) for v in graphs.called_by_iz.values())
    Path(path).write_text(f"{num_edges/total_edges:.4f}\n")


def calculate_called_by1(graphs, path):
    output = [k for k, v in graphs.called_by.items() if len(v) == 1]
    Path(path).write_text("\n".join(output) + "\n")

# another idea - draw graph without edges and node size is how much graph is called by / calling
