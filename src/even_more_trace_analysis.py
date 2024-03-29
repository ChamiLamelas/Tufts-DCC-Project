import collect_traces as ct
import misc as c
from collections import defaultdict
import os
from tqdm import tqdm




def count_missing(trace):
    return sum(((c.um(row) < 0) + (c.dm(row) < 0)) for row in trace)


def dup_rpcid_diff_um(trace):
    dups = defaultdict(list)
    for row in trace:
        dups[c.rpc(row)].append(row)
    return c.has_condition_match(dups.values(), lambda v: len({c.um(row) for row in v if c.um(row) >= 0}) > 1)


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


def save_still_missing_level(total_missing_level, total_traces, trace):
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS, "traces_still_with_missing_level.txt"),
                 f"{total_missing_level}/{total_traces} still have missing levels.\n{trace}\n")


def per_file_func(file, prereqs):
    num_dup_rpcid_diff_um, num_dup_rpcid_diff_dm, total_traces, total_orig_count, total_new_count, total_still_missing, total_missing_level = 0, 0, 0, 0, 0, 0, 0
    um_diff_t, dm_diff_t, still_missing_t, missing_level_t = None, None, None, None
    for _, trace in ct.get_trace_data(file, *prereqs).items():
        total_traces += 1
        total_orig_count += count_missing(trace)
        trace = ct.rmv_dups(trace)
        ct.fill_levels(trace)
        ct.patch_missing(trace)
        if dup_rpcid_diff_um(trace):
            num_dup_rpcid_diff_um += 1
            if um_diff_t is None:
                um_diff_t = trace
        if dup_rpcid_diff_dm(trace):
            num_dup_rpcid_diff_dm += 1
            if dm_diff_t is None:
                dm_diff_t = trace
        if ct.missing_levels(trace):
            total_missing_level += 1
            if missing_level_t is None:
                missing_level_t = trace
        new_count = count_missing(trace)
        total_new_count += new_count
        total_still_missing += new_count > 0
        if new_count > 0 and still_missing_t is None:
            still_missing_t = trace
    return [num_dup_rpcid_diff_um, num_dup_rpcid_diff_dm, total_traces, total_orig_count, total_new_count, total_still_missing, total_missing_level, um_diff_t, dm_diff_t, still_missing_t, missing_level_t]


def main():
    results = c.run_func_on_data_files(per_file_func, ct.collect_prerequisites())
    num_dup_rpcid_diff_um, num_dup_rpcid_diff_dm, total_traces, total_orig_count, total_new_count, total_still_missing, total_missing_level = 0, 0, 0, 0, 0, 0, 0
    um_diff_t, dm_diff_t, still_missing_t, missing_level_t = None, None, None, None
    for result in tqdm(results, desc="Merging results", total=len(results)):
        num_dup_rpcid_diff_um += result[0]
        num_dup_rpcid_diff_dm += result[1]
        total_traces += result[2]
        total_orig_count += result[3]
        total_new_count += result[4]
        total_still_missing += result[5]
        total_missing_level += result[6]
        if um_diff_t is None:
            um_diff_t = result[7]
        if dm_diff_t is None:
            dm_diff_t = result[8]
        if still_missing_t is None:
            still_missing_t = result[9]
        if missing_level_t is None:
            missing_level_t = result[10]
    save_num_dup_rpcid_diff_um(num_dup_rpcid_diff_um, total_traces, um_diff_t)
    save_num_dup_rpcid_diff_dm_inc_missing(
        num_dup_rpcid_diff_dm, total_traces, dm_diff_t)
    save_still_missing(total_orig_count, total_new_count,
                       total_still_missing, total_traces, still_missing_t)
    save_still_missing_level(
        total_missing_level, total_traces, missing_level_t)


if __name__ == '__main__':
    main()
