from pathlib import Path
import multiprocessing as mp
import subprocess as sp
import sys
import os
import time
from datetime import timedelta
from math import ceil

def downloader(idx):
    print(f"Starting download for file {idx}", file=sys.stderr)
    sp.run(["wget", "-c", "--retry-connrefused", "--tries=0", "--timeout=50",
           f"http://alitrip.oss-cn-zhangjiakou.aliyuncs.com/TraceData/MSCallGraph/MSCallGraph_{idx}.tar.gz"], stdout=sp.PIPE, stderr=sp.PIPE)
    print(f"Completed download for file {idx}", file=sys.stderr)
    sp.run(["tar", "-xf", f"MSCallGraph_{idx}.tar.gz", "-C", "../data"], stdout=sp.PIPE, stderr=sp.PIPE)
    os.remove(f"MSCallGraph_{idx}.tar.gz")
    print(f"Extracted zip for file {idx}", file=sys.stderr)


def download(idxs):
    ti = time.time()
    children = list()
    for idx in idxs:
        child = mp.Process(target=downloader, args=(idx,))
        child.start()
        children.append(child)
    for child in children:
        child.join()
    tf = time.time()
    print(f"All {len(idxs)} downloads finished in {str(timedelta(seconds=ceil(tf-ti)))}")

def get_idxs(arg):
    try:
        if '-' in arg:
            spl = arg.split('-')
            start = int(spl[0])
            end = int(spl[1])+1
            return list(range(start,end))
        elif ',' in arg:
            return [int(e) for e in arg.split(',')]
        return int(arg)
    except ValueError as e:
        raise ValueError('Must specify args:\n\t- A single int\n\t- A list of ints separated by , (no space)\n\t- A range of ints specified as int1-int2\n')


def main():
    Path(os.path.join("..", "data")).mkdir(parents=True, exist_ok=True)
    assert len(sys.argv) == 2
    download(get_idxs(sys.argv[1]))


if __name__ == '__main__':
    main()
