from pathlib import Path
import multiprocessing as mp
import subprocess as sp
import sys
import os


def downloader(idx):
    sp.run(["wget", "-c", "--retry-connrefused", "--tries=0", "--timeout=50",
           f"http://alitrip.oss-cn-zhangjiakou.aliyuncs.com/TraceData/MSCallGraph/MSCallGraph_${idx}.tar.gz"])
    sp.run(["tar", "-xf", f"MSCallGraph_${idx}.tar.gz", "-C", "../data"])
    os.remove(f"MSCallGraph_${idx}.tar.gz")
    print(f"Completed download for file {idx}", file=sys.stderr)


def download(start_idx, end_idx):
    children = list()
    for idx in range(start_idx, end_idx + 1):
        child = mp.Process(target=downloader, args=(idx,))
        child.start()
        children.append(child)
    for child in children:
        child.join()
    print(f"All {end_idx - start_idx + 1} downloads finished")


def main():
    Path(os.path.join("..", "data")).mkdir(parents=True, exist_ok=True)
    assert len(sys.argv) == 3
    download(int(sys.argv[1]), int(sys.argv[2]))


if __name__ == '__main__':
    main()
