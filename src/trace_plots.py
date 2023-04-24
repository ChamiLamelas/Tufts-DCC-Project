import matplotlib.pyplot as plt
import misc as c
import os
import time


def plot_concurrency_histogram(concurrencies):
    path = os.path.join(c.RESULT_FOLDER, c.TRACES, 'concurrencies.png')
    c.prep_path(path)
    plt.figure()
    plt.hist(concurrencies, bins=200)
    plt.suptitle("Distribution of Maximum Trace Concurrencies")
    plt.title(
        f"Minimum Concurrency={min(concurrencies)}, Maximum={max(concurrencies)}")
    plt.xlabel("Concurrency Width")
    plt.ylabel("Frequency")
    plt.grid()
    plt.savefig(path)


def plot_depth_histogram(depth):
    path = os.path.join(c.RESULT_FOLDER, c.TRACES, 'depths.png')
    c.prep_path(path)
    plt.figure()
    plt.hist(depth, bins=30)
    plt.suptitle("Distribution of Trace Depths")
    plt.title(f"Minimum Depth={min(depth)}, Maximum={max(depth)}")
    plt.xlabel("Depth")
    plt.ylabel("Frequency")
    plt.grid()
    plt.savefig(path)


def main():
    # Takes about 3 minutes
    concurrencies_depths = c.read_result_object(
        c.CONCURRENCIES_AND_DEPTHS_FILE)
    concurrencies = [e[0] for ls in concurrencies_depths for e in ls]
    depths = [e[1] for ls in concurrencies_depths for e in ls]
    print(sum(cn < 2 for cn in concurrencies))
    # ti = time.time()
    # plot_concurrency_histogram(concurrencies)
    # print(f"concurrencies plotted {c.prettytime(time.time() - ti)}")
    # ti = time.time()
    # plot_depth_histogram(depths)
    # print(f"depths plotted {c.prettytime(time.time() - ti)}")


if __name__ == '__main__':
    main()
