import collect_traces as ct
import misc as c
from tqdm import tqdm
import os
from collections import defaultdict
import misc as c


def dup_rpcid_diff_dm(trace):
    dups = defaultdict(list)
    for row in trace:
        dups[c.rpc(row)].append(row)
    return c.has_condition_match(dups.values(), lambda v: len({c.dm(row) for row in v}.difference({c.MISSING_MICROSERVICE})) > 1)


def has_negative_ts(trace):
    return c.has_condition_match(trace, lambda row: c.timestamp(row) < 0)


def missing_ms(trace):
    return c.has_condition_match(trace, lambda row: c.dm(row) == c.MISSING_MICROSERVICE or c.um(row) == c.MISSING_MICROSERVICE)


def missing_rpc_level(trace):
    rpcids = {c.rpc(row).count('.') for row in trace}
    return len(rpcids) < (max(rpcids) - min(rpcids) + 1)


def save_num_negative_ts(num_negative_ts, total_traces, trace):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, "traces_with_negative_timestamps.txt"),
                 f"{num_negative_ts}/{total_traces} have negative timestamps.\n{trace}\n")


def save_num_dup_rpcid_diff_dm(num_dup_rpcid_diff_dm, total_traces, trace):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, "traces_with_dup_rpcids_diff_dms.txt"),
                 f"{num_dup_rpcid_diff_dm}/{total_traces} have duplicate RPC IDs and differing (not-missing) downstream microservices.\n{trace}\n")


def save_num_uniq_ts(num_uniq_ts, total_traces, trace):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, "traces_with_uniq_rpcids_after_rmv_dups.txt"),
                 f"{num_uniq_ts}/{total_traces} have unique RPC IDs after applying rmv_dup.\n{trace}\n")


def save_num_missing_ms(num_missing_ms, total_traces, trace):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, "traces_with_missing_ms_after_patch.txt"),
                 f"{num_missing_ms}/{total_traces} has a row with at least 1 missing microservice ID after patch_missing.\n{trace}\n")


def save_num_missing_rpc_levels(num_missing_level, total_traces, trace):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, "traces_with_missing_rpc_level.txt"),
                 f"{num_missing_level}/{total_traces} is missing at least 1 RPC level.\n{trace}\n")


def main():
    prereqs = ct.collect_prerequisites()
    total_traces, num_missing_level, num_negative_ts, num_dup_rpcid_diff_dm, num_uniq_ts, num_missing_ms = 0, 0, 0, 0, 0, 0
    missing_level_t, dm_diff_t, not_uniq_t, missing_t, neg_t = None, None, None, None, None
    files = c.get_csvs()
    for file in tqdm(files, desc='Scraping Traces', total=len(files)):
        for _, trace in ct.get_trace_data(file, *prereqs).items():
            total_traces += 1
            if missing_rpc_level(trace):
                num_missing_level += 1
                if missing_level_t is None:
                    missing_level_t = trace
            if has_negative_ts(trace):
                num_negative_ts += 1
                if neg_t is None:
                    neg_t = trace
            if dup_rpcid_diff_dm(trace):
                num_dup_rpcid_diff_dm += 1
                if dm_diff_t is None:
                    dm_diff_t = trace
            trace = ct.rmv_dups(trace)
            if ct.has_unique_rpcids(trace):
                num_uniq_ts += 1
            elif not_uniq_t is None:
                not_uniq_t = trace
            ct.patch_missing(trace)
            if missing_ms(trace):
                num_missing_ms += 1
                if missing_t is None:
                    missing_t = trace
    save_num_negative_ts(num_negative_ts, total_traces, neg_t)
    save_num_dup_rpcid_diff_dm(num_dup_rpcid_diff_dm, total_traces, dm_diff_t)
    save_num_uniq_ts(num_uniq_ts, total_traces, not_uniq_t)
    save_num_missing_ms(num_missing_ms, total_traces, missing_t)
    save_num_missing_rpc_levels(
        num_missing_level, total_traces, missing_level_t)


if __name__ == '__main__':
    main()
