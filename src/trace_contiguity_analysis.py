import pandas as pd
from collections import defaultdict, Counter
import os
import misc as c
import time


def is_contiguous(nums):
    return set(range(min(nums), max(nums) + 1)) == set(nums)


def get_traces(f):
    df = pd.read_csv(os.path.join(c.DATA_FOLDER, f))
    return set(df[c.TRACE_ID].unique())


def main():
    results = c.run_func_on_data_files(get_traces)
    ti = time.time()
    traces_files = defaultdict(list)
    for i, traces in enumerate(results):
        for trace in traces:
            traces_files[trace].append(i)
    c.save_result_object(c.TRACE_FILES_FILE, list(traces_files.values()))
    c.save_result_object(c.TRACE_IDS_FILE, [t for t in traces_files])

    splits = dict(Counter(len(v) for v in traces_files.values()))
    out = [f"Split distribution of {len(traces_files)} traces: {splits}"]
    for ks in splits:
        for ktf, vtf in traces_files.items():
            if len(vtf) == ks:
                out.append(f"Example of {ks}-split: {ktf}, files: {vtf}")
                break
    contiguous_splits = sum(
        1 for v in traces_files.values() if is_contiguous(v))
    out.append(
        f"Traces split on contiguous files: {contiguous_splits}/{len(traces_files)}")
    c.write_text(os.path.join(c.RESULT_FOLDER, c.ERRORS,
                              "trace_splitting.txt"), "\n".join(out) + "\n")

    tf = time.time()
    c.debug(f"Merging and saving took " + c.prettytime(tf - ti))


if __name__ == '__main__':
    main()
