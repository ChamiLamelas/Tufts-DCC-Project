import misc as c
import collect_traces as ct
from collections import defaultdict


def no_ms_preprocess(trace):
    trace = ct.rmv_dups(trace)
    ct.fill_levels(trace)
    ct.patch_missing(trace)
    return None if ct.dup_rpcid_diff_dm(trace) or ct.dup_rpcid_diff_um(trace) else trace


def build_call_graph_no_ms(trace):
    ROOT_PREFIX = "~"
    graph = defaultdict(list)
    roots = set(ct.find_roots(trace))
    rpcdups = defaultdict(lambda: 0)
    microservices = dict()
    for i, row in enumerate(trace):
        if i in roots:
            rpckey = ROOT_PREFIX + str(i)
            microservices[rpckey] = c.um(row)
        else:
            rpckey = ct.get_parent(c.rpc(row))
        newrpc = c.rpc(row)
        if rpcdups[newrpc] > 0:
            newrpc += f"_{rpcdups[newrpc]}"
        rpcdups[c.rpc(row)] += 1
        microservices[newrpc] = c.dm(row)
        graph[rpckey].append(newrpc)
    indexing = {k: i for i, k in enumerate(microservices)}
    edgelist = [(indexing[k], indexing[v])
                for k, adj in graph.items() for v in adj]
    return edgelist, list(microservices.values())


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
        c.make_blank_row('0.1'),
        c.make_blank_row('9.1.1'),
        c.make_blank_row('9.1.1.1')
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

    print("=" * 10)

    very_weird_trace = [
        [-1, -1, '9.1.1.1', 'mc', 2],
        [2, 3, '0.1.1', 'rpc', 3],
        [-1, 1, '0', 'rpc', 1],
        [2, 3, '0.1.1', 'rpc', 3],
        [3, 4, '0.1.1.1', 'rpc', 4],
        [5, 6, '9.1.1', 'mc', 1],
        [2, 3, '0.1.1', 'rpc', -3],
        [2, 3, '0.1.2', 'rpc', 4],
        [2, 3, '0.1.1', 'rpc', 4]
    ]
    very_weird_trace = c.make_hierarchy(very_weird_trace)
    c.nice_display(very_weird_trace)
    very_weird_trace = ct.rmv_dups(very_weird_trace)
    ct.fill_levels(very_weird_trace)
    ct.patch_missing(very_weird_trace)
    callgraph, microservices = build_call_graph_no_ms(very_weird_trace)
    print(callgraph, microservices, sep='\n')
