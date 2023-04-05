from collections import defaultdict, Counter
import misc as c
import os
import collect_traces as ct
from tqdm import tqdm


def get_parent(rpcid):
    # Gets parent RPC ID
    return None if '.' not in rpcid else rpcid[:rpcid.rindex('.')]


def get_graph(trace):
    # Constructs call graph (tree) from trace
    graph = defaultdict(list)
    for row in trace:
        graph[(c.um(row), get_parent(c.rpc(row)))
              ].append((c.dm(row), c.rpc(row)))
    return dict(graph)


def count_on_condition(traces, condition):
    return sum(condition(trace) for trace in traces)


def has_unique_rpcids(trace):
    return len(trace) == len({c.rpc(row) for row in trace})


def missing_nonzero_root(trace):
    return '0' not in {c.rpc(row) for row in trace}


def rpcids_unique(traces):
    return count_on_condition(traces, has_unique_rpcids)


def save_rpcids_unique(count_unique, total_traces):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, "traces_with_unique_rpcids.txt"),
                 f"{count_unique}/{total_traces} have unique RPC IDs.")


def missing_microservice_ids(traces):
    missing_one_ms, missing_both_ms = 0, 0
    for trace in traces:
        missing_one_ms += (sum(1 for row in trace if c.um(row) ==
                               c.MISSING_MICROSERVICE or c.dm(row) == c.MISSING_MICROSERVICE) > 0)
        missing_both_ms += (sum(1 for row in trace if c.um(row) ==
                                c.MISSING_MICROSERVICE and c.dm(row) == c.MISSING_MICROSERVICE) > 0)
    return missing_one_ms, missing_both_ms


def save_missing_microservice_ids(missing_one_ms, missing_both_ms, total_traces):
    out = [
        f"{missing_one_ms}/{total_traces} traces have a row with either the upstream or downstream microservice missing",
        f"{missing_both_ms}/{total_traces} traces have a row with both the upstream and downstream microservice missing"
    ]
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS,
                              "traces_with_missing_microservice_ids.txt"), "\n".join(out))


def max_concurrency(traces):
    return [max(Counter(c.rpc(row).count('.') for row in trace).values()) for trace in traces]


def save_max_concurrency(trace_widths):
    c.save_result_object(c.CONCURRENCY_FILE, trace_widths)


def trace_depth(traces):
    return [max(c.rpc(row).count('.') for row in trace) for trace in traces]


def save_depth(depths):
    c.save_result_object(c.DEPTH_FILE, depths)


def traces_with_nonzero_root(traces):
    return count_on_condition(traces, missing_nonzero_root)


def save_nonzero_root(nonzero_count, total_traces):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, 'traces_with_nonzero_root.txt'),
                 f"{nonzero_count}/{total_traces} have nonzero root RPC IDs.")


def main():
    prereqs = ct.collect_prerequisites()
    total_missing_one, total_missing_both, total_traces, total_uniq_rpcids, total_nonzero_root = 0, 0, 0, 0, 0
    concurrencies, depths = list(), list()
    files = c.get_csvs()[:3]
    for file in tqdm(files, desc='Scraping Traces', total=len(files)):
        traces = list(ct.get_trace_data(file, *prereqs).values())
        total_traces += len(traces)
        total_uniq_rpcids += rpcids_unique(traces)
        miss1, missboth = missing_microservice_ids(traces)
        total_nonzero_root += traces_with_nonzero_root(traces)
        total_missing_one += miss1
        total_missing_both += missboth
        concurrencies.extend(max_concurrency(traces))
        depths.extend(trace_depth(traces))
    save_rpcids_unique(total_uniq_rpcids, total_traces)
    save_missing_microservice_ids(
        total_missing_one, total_missing_both, total_traces)
    save_max_concurrency(concurrencies)
    save_depth(depths)
    save_nonzero_root(total_nonzero_root, total_traces)


if __name__ == '__main__':
    main()
