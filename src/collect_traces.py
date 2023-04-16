import pandas as pd
import os
import misc as c
from collections import defaultdict


def get_trace_data(path, ms_integer_map, trace_integer_map, to_remove):
    df = pd.read_csv(os.path.join(c.DATA_FOLDER, path))
    df = df[c.TRACE_COLUMNS]
    df[c.TRACE_ID] = df[c.TRACE_ID].apply(
        lambda x: trace_integer_map[x]).astype('int')
    df = df[~df[c.TRACE_ID].isin(to_remove)]
    df[c.UPSTREAM_ID] = df[c.UPSTREAM_ID].apply(
        lambda x: ms_integer_map[x]).astype('int')
    df[c.DOWNSTREAM_ID] = df[c.DOWNSTREAM_ID].apply(
        lambda x: ms_integer_map[x]).astype('int')
    as_dict = df.groupby([c.TRACE_ID])[c.TRACE_COLUMNS[1:]].apply(
        lambda x: x.values.tolist()).to_dict()
    return as_dict


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
    return [row for row in trace if c.rpc(row).startswith(rpcid) and c.rpc(row).count('.') == rpcid.count('.') + 1]


def get_parent_rows(rpcid, trace):
    parentid = get_parent(rpcid)
    return [(i, row) for i, row in enumerate(trace) if c.rpc(row) == parentid]


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


def has_unique_rpcids(trace):
    return len(trace) == len({c.rpc(row) for row in trace})


def _find_root(trace):
    # Identify root by find row in trace that has the minimum number of
    # dots in the RPCID, we return that index
    return min(list(range(len(trace))), key=lambda i: c.rpc(trace[i]).count('.'))


def _fill_to_root(trace, rowidx, can_reach_parent):
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
            row = c.make_blank_row(get_parent(c.rpc(trace[rowidx])))
            trace.append(row)

            # We haven't determined if this new row's parent is in
            # the trace so we put a False entry for it in the table
            can_reach_parent.append(False)

            # Next iteration we will find the new row's parent
            rowidx = len(can_reach_parent) - 1
        else:
            # Next iteration we will see if the discovered parent
            # can reach its parent (this allows us to terminate
            # this loop early instead of tracing all the way up
            # to the root of the trace again)
            rowidx = parents[0][0]


def fill_levels(trace):
    rootidx = _find_root(trace)

    # Table marking whether we have determined that ith row of trace 
    # can reach its parent
    can_reach_parent = [False] * len(trace)

    # Mark a special case -- the root of the trace has no parent
    # so we should never bother trying to find it in _fill_to_root 
    # hence we mark it as being able to reach its parent
    can_reach_parent[rootidx] = True

    # _fill_to_root will expand trace, so we iterate over
    # the original number of rows in trace and make sure 
    # each of those rows can go to the root, any new added
    # rows will be checked by _fill_to_root
    rows = len(trace)
    for i in range(rows):
        _fill_to_root(trace, i, can_reach_parent)


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
        if c.dm(trace[i]) != c.MISSING_MICROSERVICE and c.um(trace[i]) != c.MISSING_MICROSERVICE:
            continue

        # If UM is missing, find the parents of this row in the trace (parents because of the
        # duplicate RPCID possibility)
        if c.um(trace[i]) == c.MISSING_MICROSERVICE:
            parents = get_parent_rows(c.rpc(trace[i]), trace)

            # If there isnt 1 parent -- cannot fix. Because, there could be multiple
            # DMs of parent rows
            if len(parents) != 1:
                continue

            # Otherwise set UM to be the DM of the 1 and only parent
            c.set_um(trace[i], c.dm(parents[0]))

        # If DM is missing, find the children of the row. Then get the unique not missing UM
        # of all the children. If there is not 1 non-missing UM of the children, cannot fix
        if c.dm(trace[i]) == c.MISSING_MICROSERVICE:
            child_ums = list({c.um(row) for row in get_child_rows(
                c.rpc(trace[i]), trace)}.difference({c.MISSING_MICROSERVICE}))
            if len(child_ums) != 1:
                continue

            # Otherwise set 1 and only child UM
            c.set_dm(trace[i], child_ums[0])
