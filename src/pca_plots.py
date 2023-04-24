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
    print(f"... ploting scatters for {len(eigen_vecs)} microservices ...")

    # plot clusters in one fig
    path = os.path.join(embed_path, 'MSCallGraph_'+str(file_id)+'_pca.png')
    c.prep_path(path)
    cmap = plt.get_cmap('gnuplot')
    colors = [cmap(i) for i in np.linspace(0, 1, len(eigen_vecs))]
    plt.figure()
    for i, k in enumerate(eigen_vecs):
        plt.scatter(eigen_vecs[k][0], eigen_vecs[k][1], s=1, c=colors[i], label=k)
    plt.legend(loc='best', prop={'size': 8})
    plt.title("PCA Eigenvectors Colored By Service")
    plt.grid()
    plt.savefig(path)

    # plot clusters for each service
    for k in eigen_vecs:
        path = os.path.join(embed_path, 'microservice_'+k+'_pca.png')
        c.prep_path(path)
        plt.figure()
        plt.scatter(eigen_vecs[k][0], eigen_vecs[k][1], s=1)
        plt.title("PCA Eigenvector Clusters for Microservice "+k)
        plt.grid()
        plt.savefig(path)

def main():
    plot_eigenvecs_clusters(128)

if __name__ == '__main__':
    main()