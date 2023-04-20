import misc as c
import collect_traces as ct
from collections import defaultdict


def build_call_graph_no_ms(trace):
    ROOT_KEY = ""
    graph = defaultdict(list)
    root = ct.find_root(trace)
    for i, row in enumerate(trace):
        if i == root:
            graph[ROOT_KEY].append(c.rpc(row))
        else:
            graph[ct.get_parent(c.rpc(row))].append(c.rpc(row))
    indexing = {k: i for i, k in enumerate(
        set(graph).union(set(v for adj in graph.values() for v in adj)))}
    return [(indexing[k], indexing[v]) for k, adj in graph.items() for v in adj]


def build_call_graph_ms(trace):
    graph = defaultdict(list)
    for row in trace:
        graph[(c.um(row), ct.get_parent(c.rpc(row)))
              ].append((c.dm(row), c.rpc(row)))
    return graph


if __name__ == '__main__':
    nice_trace = c.make_hierarchy([
        c.make_blank_row('0.1'),
        c.make_blank_row('0.1.1'),
        c.make_blank_row('0'),
        c.make_blank_row('0.1.2')
    ])
    c.nice_display(nice_trace)
    graph = build_call_graph_no_ms(nice_trace)
    print(graph)

    weird_trace = c.make_hierarchy([
        c.make_blank_row('0.1.1'),
        c.make_blank_row('0.1.1.1'),
        c.make_blank_row('0.1.1'),
        c.make_blank_row('0'),
        c.make_blank_row('0.1')
    ])
    c.nice_display(weird_trace)
    graph = build_call_graph_no_ms(weird_trace)
    print(graph)

    very_nice_trace = c.make_hierarchy([
        [1, 2, '0.1', 'rpc', 1],
        [0, 1, '0', 'rpc', 1],
        [2, 3, '0.1.1', 'rpc', 1],
        [2, 4, '0.1.2', 'rpc', 1],
        [1, 3, '0.2', 'rpc', 1],
        [3, 4, '0.2.1', 'rpc', 1]
    ])
    c.nice_display(very_nice_trace)
    graph = build_call_graph_ms(very_nice_trace)
    for k, v in graph.items():
        print(str(k) + ":\n\t" + ", ".join(str(e) for e in v))
    
