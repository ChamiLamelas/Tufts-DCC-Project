import collect_traces as ct
import misc as c
import sys
import time


def get_traces_with_unique_rpcids(traces):
    return c.dict_val_filter(ct.has_unique_rpcids, traces)


def main():
    ti = time.time()
    samples = c.get_cmdline_arg(int)
    prereqs = ct.collect_prerequisites()
    nice_traces = dict()
    files = c.get_csvs()
    for i, file in enumerate(files):
        traces = ct.get_trace_data(file, *prereqs)
        nice_traces.update(get_traces_with_unique_rpcids(traces))
        print(
            f"Progress: {len(nice_traces)}/{samples} Files: {i + 1}/{len(files)}", file=sys.stderr)
        if len(nice_traces) >= samples:
            break
    c.save_object(c.NICE_TRACES_FILE, c.dict_get_n(nice_traces, samples))
    print(f"Time: {c.prettytime(time.time() - ti)}")


if __name__ == '__main__':
    main()
