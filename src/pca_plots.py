import matplotlib.pyplot as plt
import misc as c
import os
import json
import numpy as np

def plot_eigenvecs_clusters(file_id):
    # read json
    embed_path = os.path.join(c.RESULT_FOLDER, c.EMBEDDINGS)
    file_name = os.path.join(embed_path, 'MSCallGraph_'+str(file_id)+'.csv.pca.json')
    eigen_vecs = json.load(open(file_name, 'r'))

    # plot clusters
    path = os.path.join(embed_path, str(file_id)+'_pca.png')
    c.prep_path(path)
    cmap = plt.get_cmap('gnuplot')
    colors = [cmap(i) for i in np.linspace(0, 1, len(eigen_vecs))]
    plt.figure()
    for i, k in enumerate(eigen_vecs):
        plt.scatter(eigen_vecs[k][0], eigen_vecs[k][1], s=1, c=colors[i], label=k)
    plt.legend(loc='best', prop={'size': 8})
    plt.title("PCA Eigenvectors Clustered By Service")
    plt.grid()
    plt.savefig(path)

def main():
    # plot_concurrency_histogram()
    # plot_depth_histogram()
    # lookat_concurrencies()
    plot_eigenvecs_clusters(128)


if __name__ == '__main__':
    main()