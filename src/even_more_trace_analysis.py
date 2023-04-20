import collect_traces as ct
import misc as c
from tqdm import tqdm
from collections import defaultdict
import os


def count_missing(trace):
    return sum(c.um(row) == c.MISSING_MICROSERVICE + c.dm(row) == c.MISSING_MICROSERVICE for row in trace)


def dup_rpcid_diff_um(trace):
    dups = defaultdict(list)
    for row in trace:
        dups[c.rpc(row)].append(row)
    return c.has_condition_match(dups.values(), lambda v: len({c.um(row) for row in v}.difference({c.MISSING_MICROSERVICE})) > 1)


def dup_rpcid_diff_dm(trace):
    dups = defaultdict(list)
    for row in trace:
        dups[c.rpc(row)].append(row)
    return c.has_condition_match(dups.values(), lambda v: len({c.dm(row) for row in v}) > 1)


def save_num_dup_rpcid_diff_dm_inc_missing(num_dup_rpcid_diff_dm, total_traces, trace):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, "traces_with_dup_rpcids_diff_dms_inc_missing.txt"),
                 f"{num_dup_rpcid_diff_dm}/{total_traces} have duplicate RPC IDs and differing downstream microservices (including missing).\n{trace}\n")


def save_num_dup_rpcid_diff_um(num_dup_rpcid_diff_um, total_traces, trace):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, "traces_with_dup_rpcids_diff_ums.txt"),
                 f"{num_dup_rpcid_diff_um}/{total_traces} have duplicate RPC IDs and differing (not-missing) upstream microservices.\n{trace}\n")


def save_still_missing(total_orig, total_new, total_still_missing, total_traces, trace):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, "traces_with_missing_ms_after_patch_2.txt"),
                 f"{total_still_missing}/{total_traces} still have missing microservices.\n{total_orig} microservices were missing. Now {total_new} microservices are still missing.\n{trace}\n")


def main():
    prereqs = ct.collect_prerequisites()
    files = c.get_csvs()[:2]
    num_dup_rpcid_diff_um, num_dup_rpcid_diff_dm, total_traces, total_orig_count, total_new_count, total_still_missing = 0, 0, 0, 0, 0, 0
    um_diff_t, dm_diff_t, still_missing_t = None, None, None
    for file in tqdm(files, desc='Scraping Traces', total=len(files)):
        for _, trace in ct.get_trace_data(file, *prereqs).items():
            total_traces += 1
            total_orig_count += count_missing(trace)
            trace = ct.rmv_dups(trace)
            ct.patch_missing(trace)
            if dup_rpcid_diff_um(trace):
                num_dup_rpcid_diff_um += 1
                if um_diff_t is None:
                    um_diff_t = trace
            if dup_rpcid_diff_dm(trace):
                num_dup_rpcid_diff_dm += 1
                if dm_diff_t is None:
                    dm_diff_t = trace
            new_count = count_missing(trace)
            total_new_count += new_count
            total_still_missing += new_count > 0
            if new_count > 0 and still_missing_t is None:
                still_missing_t = trace
    save_num_dup_rpcid_diff_um(num_dup_rpcid_diff_um, total_traces, um_diff_t)
    save_num_dup_rpcid_diff_dm_inc_missing(
        num_dup_rpcid_diff_dm, total_traces, dm_diff_t)
    save_still_missing(total_orig_count, total_new_count,
                       total_still_missing, total_traces, still_missing_t)

if __name__ == '__main__':
    main()