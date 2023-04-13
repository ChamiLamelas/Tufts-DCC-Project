import collect_traces as ct
import misc as c
import sys
import time


def get_traces_with_not_unique_rpcids(traces):
    return c.dict_val_filter(lambda trace: not ct.has_unique_rpcids(trace), traces)


def missing_one_ms(trace):
    return any(c.um(row) == c.MISSING_MICROSERVICE or c.dm(row) == c.MISSING_MICROSERVICE for row in trace)


def missing_both_ms(trace):
    return any(c.um(row) == c.MISSING_MICROSERVICE and c.dm(row) == c.MISSING_MICROSERVICE for row in trace)


def get_traces_with_one_missing_ms(traces):
    return c.dict_val_filter(missing_one_ms, traces)


def get_traces_with_both_missing_ms(traces):
    return c.dict_val_filter(missing_both_ms, traces)


def main():
    ti = time.time()
    samples = c.get_cmdline_arg(int)
    prereqs = ct.collect_prerequisites()
    not_uniq, missing_one, missing_both = dict(), dict(), dict()
    files = c.get_csvs()
    for i, file in enumerate(files):
        traces = ct.get_trace_data(file, *prereqs)
        if len(not_uniq) < samples:
            not_uniq.update(get_traces_with_not_unique_rpcids(traces))
        if len(missing_one) < samples:
            missing_one.update(get_traces_with_one_missing_ms(traces))
        if len(missing_both) < samples:
            missing_both.update(get_traces_with_both_missing_ms(traces))
        print(
            f"Progress: Not Unique: {len(not_uniq)} Missing 1: {len(missing_one)} Missing 2: {len(missing_both)} (Out of {samples}) Files: {i + 1}/{len(files)}", file=sys.stderr)
        if len(not_uniq) >= samples and len(missing_one) >= samples and len(missing_both) >= samples:
            break
    c.save_object(c.NON_UNIQ_TRACES_FILE, c.dict_get_n(not_uniq, samples))
    c.save_object(c.MISSING_MS_TRACES_FILE, c.dict_get_n(missing_one, samples))
    c.save_object(c.MISSING_BOTH_TRACES_FILE,
                  c.dict_get_n(missing_both, samples))
    print(f"Time: {c.prettytime(time.time() - ti)}")


if __name__ == '__main__':
    main()
