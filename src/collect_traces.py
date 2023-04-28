import pandas as pd
import os
import misc as c
from collections import defaultdict, Counter
import sys


def get_trace_data(path, ms_integer_map, trace_integer_map, to_remove):
    df = pd.read_csv(os.path.join(c.DATA_FOLDER, path))[c.TRACE_COLUMNS]
    df[c.RPC_ID] = df[c.RPC_ID].str.replace(":", "")
    df[c.TRACE_ID] = df[c.TRACE_ID].apply(
        lambda x: trace_integer_map[x]).astype('int')
    df = df[~df[c.TRACE_ID].isin(to_remove)]
    df[c.UPSTREAM_ID] = df[c.UPSTREAM_ID].apply(
        lambda x: ms_integer_map[x]).astype('int')
    df[c.DOWNSTREAM_ID] = df[c.DOWNSTREAM_ID].apply(
        lambda x: ms_integer_map[x]).astype('int')
    as_dict = df.groupby([c.TRACE_ID])[c.TRACE_COLUMNS[1:]].apply(
        lambda x: x.values.tolist()).to_dict()
    return {tid: c.make_hierarchy(trace) for tid, trace in as_dict.items()}


def collect_prerequisites():
    microservices = c.read_result_object(c.MICROSERVICES_FILE)
    traces = c.read_result_object(c.TRACE_IDS_FILE)
    trace_files = c.read_result_object(c.TRACE_FILES_FILE)
    ms_integer_map = defaultdict(lambda: c.MISSING_MICROSERVICE)
    ms_integer_map.update({m: i for i, m in enumerate(microservices)})
    trace_integer_map = {t: i for i, t in enumerate(traces)}
    to_remove = {i for i, files in enumerate(trace_files) if len(files) > 1}
    return ms_integer_map, trace_integer_map, to_remove


def get_child_rows(rpcid, trace):
    child_dots = rpcid.count('.') + 1
    return [row for row in trace if c.rpc(row).startswith(rpcid) and c.rpc(row).count('.') == child_dots]


def get_parent_rows(rpcid, trace):
    parentid = get_parent(rpcid)
    return [] if parentid is None else [(i, row) for i, row in enumerate(trace) if c.rpc(row) == parentid]


def get_parent(rpcid):
    # Gets parent RPC ID
    return None if '.' not in rpcid else rpcid[:rpcid.rindex('.')]


def has_unique_rpcids(trace):
    return len(trace) == len({c.rpc(row) for row in trace})


def dup_rpcid_diff_um(trace):
    dups = defaultdict(list)
    for row in trace:
        dups[c.rpc(row)].append(row)
    return c.has_condition_match(dups.values(), lambda v: len({c.um(row) for row in v}) > 1)


def dup_rpcid_diff_dm(trace):
    dups = defaultdict(list)
    for row in trace:
        dups[c.rpc(row)].append(row)
    return c.has_condition_match(dups.values(), lambda v: len({c.dm(row) for row in v}) > 1)


def isancestor(rpc1, rpc2):
    return rpc1.startswith(rpc2) and rpc1[len(rpc2):].find('.') == 0


def find_roots(trace):
    # Identify all indices with minimum number of dots in rpcid

    no_parents = [i for i, row in enumerate(
        trace) if len(get_parent_rows(c.rpc(row), trace)) == 0]
    return [i for i in no_parents if not any(isancestor(c.rpc(trace[i]), c.rpc(trace[j])) for j in no_parents)]


def _fill_to_root(trace, rowidx, can_reach_parent, min_root_level):
    # We start at some arbitrary row in the trace, first we check
    # if we have determined if the parent is in the trace
    while not can_reach_parent[rowidx]:
        # If we haven't determined it yet, mark that we have
        # determined it -- as this is what will happen below
        can_reach_parent[rowidx] = True

        # Search for parent by RPCID in the trace
        parents = get_parent_rows(c.rpc(trace[rowidx]), trace)

        # If no parent found, add a new blank row to the trace that
        # would be this row's parent
        if len(parents) == 0:
            newid = get_parent(c.rpc(trace[rowidx]))
            row = c.make_blank_row(newid)
            trace.append(row)

            # We haven't determined if this new row's parent is in
            # the trace so we put a False entry for it in the table
            can_reach_parent.append(newid.count('.') == min_root_level)

            # Next iteration we will find the new row's parent
            rowidx = len(can_reach_parent) - 1
        else:
            # Next iteration we will see if the discovered parent
            # can reach its parent (this allows us to terminate
            # this loop early instead of tracing all the way up
            # to the root of the trace again)
            rowidx = parents[0][0]


def fill_levels(trace):
    rootidxs = find_roots(trace)

    # Table marking whether we have determined that ith row of trace
    # can reach its parent
    can_reach_parent = [False] * len(trace)

    # Mark a special case -- the root of the trace has no parent
    # so we should never bother trying to find it in _fill_to_root
    # hence we mark it as being able to reach its parent
    min_root_level = None
    for rootidx in rootidxs:
        can_reach_parent[rootidx] = True
        curr_dots = c.rpc(trace[rootidx]).count('.')
        if min_root_level is None or curr_dots < min_root_level:
            min_root_level = curr_dots

    # _fill_to_root will expand trace, so we iterate over
    # the original number of rows in trace and make sure
    # each of those rows can go to the root, any new added
    # rows will be checked by _fill_to_root
    rows = len(trace)
    for i in range(rows):
        _fill_to_root(trace, i, can_reach_parent, min_root_level)


def rmv_dups(trace):
    newtrace = list()
    # both of these dicts map (um,dm,rpcid) -> list of all rows
    # that match on this key
    non_rpc_dups, rpc_dups = defaultdict(list), defaultdict(list)
    for row in trace:
        key = tuple(row[:3])
        # based on rpc type split into appropriate dictionary
        if c.rpctype(row) == 'http' or c.rpctype(row) == 'rpc':
            rpc_dups[key].append(row)
        else:
            non_rpc_dups[key].append(row)
    for v in rpc_dups.values():
        # for rpc type rpcs, if there was only 1 row add it (i.e. there
        # was no duplicate)
        if len(v) == 1:
            newtrace.append(v[0])
        # Otherwise, collect positive and negative timestamps of the
        # duplicates. Add rows of whatever the majority of positive
        # or negative are
        else:
            pos, neg = list(), list()
            for row in v:
                if c.timestamp(row) < 0:
                    neg.append(row)
                else:
                    pos.append(row)
            newtrace.extend(pos if len(pos) > len(neg) else neg)
    # for non rpc duplicates, we shouldn't have duplicates, so just
    # drop the duplicates and grab 1 of them
    newtrace.extend(v[0] for v in non_rpc_dups.values())
    return newtrace


def patch_missing(trace):
    for i in range(len(trace)):
        # Row isn't missing anything - skip it
        if c.dm(trace[i]) >= 0 and c.um(trace[i]) >= 0:
            continue

        # If UM is missing, find the parents of this row in the trace (parents because of the
        # duplicate RPCID possibility)
        if c.um(trace[i]) < 0:

            # Get the set of downstream microservices of the parents -- there needs
            # to be exactly 1 (0 means its the root - if root is missing UM, nothing
            # we can do; 2+ means the parents could have different DMs hence we can't
            # fix in this manner)
            parent_dms = {c.dm(parent) for _, parent in get_parent_rows(
                c.rpc(trace[i]), trace)}

            if len(parent_dms) != 1:
                continue

            # Set UM to be 1 and only parent DM
            c.set_um(trace[i], next(iter(parent_dms)))

        # If DM is missing, find the children of the row. Then get the unique not missing UM
        # of all the children.
        if c.dm(trace[i]) < 0:

            # Collect child UMs to fill in the DM -- ignore missing microservices
            # Again - there should only be 1 if there are 0, there are no children we
            # can use to fix, if there are multiple child UMs that are not missing
            # we can't fix either
            child_ums = {c.um(row) for row in get_child_rows(
                c.rpc(trace[i]), trace) if c.um(row) >= 0}

            if len(child_ums) != 1:
                continue

            # Set DM to be 1 and only child UM
            c.set_dm(trace[i], next(iter(child_ums)))


def missing_levels(trace):
    rpcids = {c.rpc(row) for row in trace}
    dotcounts = defaultdict(set)
    for rpcid in rpcids:
        for other in rpcids:
            if rpcid.startswith(other):
                dotcounts[rpcid].add(other.count('.'))
    return any(not c.iscontiguous(v) for v in dotcounts.values())


def count_missing(trace):
    return sum(((c.um(row) < 0) + (c.dm(row) < 0)) for row in trace)


def get_max_concurrency(trace_tree_rpcs):
    return max(Counter(rpc.count('.') for rpc in trace_tree_rpcs).values())


def get_depth(trace_tree_rpcs):
    dot_counts = [rpc.count('.') for rpc in trace_tree_rpcs]
    return max(dot_counts) - min(dot_counts) + 1


def get_max_concurrency_and_depth(trace):
    roots = find_roots(trace)
    trees = defaultdict(list)
    for row in trace:
        for rootidx in roots:
            if isancestor(c.rpc(row), c.rpc(trace[rootidx])):
                trees[c.rpc(trace[rootidx])].append(c.rpc(row))
    return max(get_max_concurrency(tree) for tree in trees.values()), max(get_depth(tree) for tree in trees.values())
