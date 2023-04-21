import matplotlib.pyplot as plt
import misc as c
import os
from scipy.stats import pearsonr
import networkx as nx

CALL_DISTRIBUTIONS = 'call_distributions'
SUMMARY_STATISTICS = 'summary_statistics'
CORRELATIONS = 'correlations'


def plot_death_star(calling):
    path = os.path.join(c.RESULT_FOLDER, CALL_DISTRIBUTIONS, 'deathstar.png')
    c.prep_path(path)
    g = nx.DiGraph()
    plt.figure()
    for u, adj in calling.items():
        # if len(adj) > 1:
        for v in adj:
            g.add_edge(u, v)
    print(g.number_of_nodes(), len(calling))
    options = {
        "node_color": "black",
        "node_size": 1,
        "edge_color": "gray",
        "linewidths": 0,
        "width": 0.1,
    }
    nx.draw_circular(g, **options)
    plt.savefig(path)


def plot_trace_histogram(traces):
    print(sum(v < 10000 for v in traces.values()), max(traces.values()))
    path = os.path.join(c.RESULT_FOLDER, CALL_DISTRIBUTIONS, 'traces.png')
    c.prep_path(path)
    plt.figure()
    d = list(traces.values())
    plt.hist(d, bins=200)
    plt.suptitle("Distribution of Microservice Trace Occurrence")
    plt.title(f"Minimum Occurrence Count={min(d)}, Maximum={max(d)}")
    plt.xlabel("Number of Traces Microservice Occurred In")
    plt.ylabel("Frequency")
    plt.grid()
    plt.savefig(path)


def plot_call_histograms(called_by, calling):
    paths = (os.path.join(c.RESULT_FOLDER, CALL_DISTRIBUTIONS, p)
             for p in ('called_by.png', 'calling.png'))
    for p, n, g in zip(paths, ('Called By', 'Calling'), (called_by, calling)):
        c.prep_path(p)
        plt.figure()
        d = [len(v) for v in g.values()]
        plt.hist(d, bins=200)
        plt.suptitle(n + " Distribution")
        plt.title(f"Minimum {n} Count={min(d)}, Maximum={max(d)}")
        plt.xlabel(n)
        plt.ylabel("Frequency")
        plt.grid()
        plt.savefig(p)


def calculate_sparsity_ratio(count_microservices, called_by):
    total_edges = count_microservices ** 2
    num_edges = sum(len(v) for v in called_by.values())
    c.write_text(os.path.join(c.RESULT_FOLDER, SUMMARY_STATISTICS,
                              "sparsity_ratio.txt"), f"{num_edges/total_edges:.4f}\n")


def calculate_called_by1(called_by):
    output = [k for k, v in called_by.items() if len(v) == 1]
    c.write_text(os.path.join(c.RESULT_FOLDER, SUMMARY_STATISTICS, 'called_by1.txt'),
                 f"{len(output)} microservices are called by only 1 other microservice.\n")


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


def calculate_connected_components(called_by, calling):
    undirected = called_by.copy()
    for k, v in calling.items():
        undirected[k] = list(set(undirected[k]).union(
            set(v))) if k in undirected else v.copy()
    unexplored = set(undirected)
    connected_sizes = list()
    while len(unexplored) > 0:
        connected_sizes.append(__helper_bfs(unexplored, undirected))
    c.write_text(os.path.join(c.RESULT_FOLDER, SUMMARY_STATISTICS, "connected_components.txt"), str(
        sorted(connected_sizes, reverse=True)) + "\n")


def calculate_correlation(called_by, calling, traces):
    paths = [os.path.join(c.RESULT_FOLDER, CORRELATIONS, p)
             for p in ('called_by_and_trace_freq.png', 'calling_and_trace_freq.png')]
    c.prep_path(paths[0])
    c.prep_path(paths[1])
    x1 = list()
    y1 = list()
    for k, v in called_by.items():
        x1.append(len(v))
        y1.append(traces[k])
    plt.figure()
    plt.xlabel("Called By Degree")
    plt.ylabel("Number of Containing Traces")
    plt.scatter(x1, y1)
    plt.suptitle("Correlation between Called-By and Trace Frequency")
    plt.title(f"r = {pearsonr(x1, y1)[0]:.4f}")
    plt.grid()
    plt.savefig(paths[0])
    x2 = list()
    y2 = list()
    for k, v in calling.items():
        x2.append(len(v))
        y2.append(traces[k])
    plt.figure()
    plt.xlabel("Calling Degree")
    plt.ylabel("Number of Containing Traces")
    plt.scatter(x2, y2)
    plt.suptitle("Correlation between Calling and Trace Frequency")
    plt.title(f"r = {pearsonr(x2, y2)[0]:.4f}")
    plt.grid()
    plt.savefig(paths[1])


def main():
    called_by = c.read_result_object(c.CALLED_BY_FILE)
    calling = c.read_result_object(c.CALLING_FILE)
    traces = c.read_result_object(c.TRACES_FILE)
    count_microservices = len(c.read_result_object(c.MICROSERVICES_FILE))
    plot_call_histograms(called_by, calling)
    plot_trace_histogram(traces)
    calculate_sparsity_ratio(count_microservices, called_by)
    calculate_called_by1(called_by)
    calculate_connected_components(called_by, calling)
    calculate_correlation(called_by, calling, traces)
    plot_death_star(calling)


if __name__ == '__main__':
    main()
