from collections import Counter
import misc as c
import os
import collect_traces as ct
from tqdm import tqdm


def missing_nonzero_root(trace):
    return '0' not in {c.rpc(row) for row in trace}


def rpcids_unique(traces):
    return c.count_on_condition(traces, ct.has_unique_rpcids)


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


# This is no longer correct now that we identified traces can have multiple roots! 
# What is concurrency of 2 root tree? Not sure how to compute
def max_concurrency(traces):
    return [max(Counter(c.rpc(row).count('.') for row in trace).values()) for trace in traces]


def save_max_concurrency(trace_widths):
    c.save_result_object(c.CONCURRENCY_FILE, trace_widths)


# This is no longer correct now that we identified traces can have multiple roots! 
# The depth of trace 0 -> 0.1  9.1.1 -> 9.1.1.2 does not depend on 9.1.1.2 (3 dots)
def trace_depth(traces):
    return [max(c.rpc(row).count('.') for row in trace) for trace in traces]


def save_depth(depths):
    c.save_result_object(c.DEPTH_FILE, depths)


def traces_with_nonzero_root(traces):
    return c.count_on_condition(traces, missing_nonzero_root)


def save_nonzero_root(nonzero_count, total_traces):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, 'traces_with_nonzero_root.txt'),
                 f"{nonzero_count}/{total_traces} have nonzero root RPC IDs.")


def per_file_func(file, prereqs):
    traces = list(ct.get_trace_data(file, *prereqs).values())
    miss1, missboth = missing_microservice_ids(traces)
    return [len(traces), rpcids_unique(traces), traces_with_nonzero_root(traces), miss1, missboth, max_concurrency(traces), trace_depth(traces)]


def main():
    results = c.run_func_on_data_files(
        per_file_func, ct.collect_prerequisites())
    total_missing_one, total_missing_both, total_traces, total_uniq_rpcids, total_nonzero_root = 0, 0, 0, 0, 0
    concurrencies, depths = list(), list()
    for result in tqdm(results, desc="Merging results", total=len(results)):
        total_traces += result[0]
        total_uniq_rpcids += result[1]
        total_nonzero_root += result[2]
        total_missing_one += result[3]
        total_missing_both += result[4]
        concurrencies.extend(result[5])
        depths.extend(result[6])
    save_rpcids_unique(total_uniq_rpcids, total_traces)
    save_missing_microservice_ids(
        total_missing_one, total_missing_both, total_traces)
    save_max_concurrency(concurrencies)
    save_depth(depths)
    save_nonzero_root(total_nonzero_root, total_traces)


if __name__ == '__main__':
    main()
