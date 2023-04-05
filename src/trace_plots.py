import matplotlib.pyplot as plt
import misc as c
import os


def plot_concurrency_histogram():
    concurrencies = c.read_result_object(c.CONCURRENCY_FILE)
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


def plot_depth_histogram():
    depth = c.read_result_object(c.DEPTH_FILE)
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
    plot_concurrency_histogram()
    plot_depth_histogram()


if __name__ == '__main__':
    main()
