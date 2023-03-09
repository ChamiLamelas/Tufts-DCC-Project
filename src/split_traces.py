import analysis as a
import pandas as pd
from collections import defaultdict, Counter
from pathlib import Path
import os


def is_contiguous(nums):
    return set(range(min(nums), max(nums) + 1)) == set(nums)


csvs = a.get_csvs()
traces_files = defaultdict(list)
for i, csv in enumerate(csvs):
    trace_ids = list(pd.read_csv(csv)[a.TRACE_ID].unique())
    for trace_id in trace_ids:
        traces_files[trace_id].append(i)
splits = dict(Counter(len(v) for v in traces_files.values()))
out = [f"Split distribution of {len(traces_files)} traces: {splits}"]
for ks in splits:
    for ktf, vtf in traces_files.items():
        if len(vtf) == ks:
            out.append(f"Example of {ks}-split: {ktf}, files: {vtf}")
            break
contiguous_splits = sum(1 for v in traces_files.values() if is_contiguous(v))
out.append(
    f"Traces split on contiguous files: {contiguous_splits}/{len(traces_files)}")
Path(os.path.join("..", "results", "traces_split.txt")
     ).write_text("\n".join(out) + "\n")
