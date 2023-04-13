import misc as c
from collections import defaultdict
import collect_traces as ct


def get_rpcid_duplicates(trace):
    dups = defaultdict(list)
    for row in trace:
        dups[c.rpc(row)].append(row)
    return c.dict_val_filter(lambda v: len(v) > 1, dups)





def main():
    non_uniq = c.read_object(c.NON_UNIQ_TRACES_FILE)
    missing1 = c.read_object(c.MISSING_MS_TRACES_FILE)

    tid, t = c.dict_get_1(missing1)
    print(t)
    ct.patch_missing(t)
    # t[1][1] = 6604
    print(t)
    print(missing_rpc_level(t))
    # print(dup_rpcid_diff_dm(t))

    # missing2 = c.read_object(c.MISSING_BOTH_TRACES_FILE)
    # for tid, t in missing1.items():
    #     print(f'Trace ID={tid}\nOriginal={t}\nPatched={patch1_missing(t)}')
    #     break

    # for rpcid, rows in get_rpcid_duplicates(t).items():
    #     print(tid, rpcid, "dups=" + str(rows), "children=" +
    #           str(ct.get_child_rows(rpcid, t)), sep='\n')
    #     return


if __name__ == '__main__':
    main()
