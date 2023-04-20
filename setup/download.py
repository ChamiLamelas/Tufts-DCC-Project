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


def download(start_idx, end_idx):
    ti = time.time()
    children = list()
    for idx in range(start_idx, end_idx + 1):
        child = mp.Process(target=downloader, args=(idx,))
        child.start()
        children.append(child)
    for child in children:
        child.join()
    tf = time.time()
    print(f"All {end_idx - start_idx + 1} downloads finished in {str(timedelta(seconds=ceil(tf-ti)))}")


def main():
    Path(os.path.join("..", "data")).mkdir(parents=True, exist_ok=True)
    assert len(sys.argv) == 3
    download(int(sys.argv[1]), int(sys.argv[2]))


if __name__ == '__main__':
    main()
