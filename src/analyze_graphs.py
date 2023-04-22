# for reading pkl file graphs
import misc as c
import os
# for embedding
import networkx as nx
from karateclub import Graph2Vec
# for PAC
from sklearn.decomposition import PCA
import numpy as np
# for plotting
from pathlib import Path
import matplotlib.pyplot as plt


def main():
    # get list of graphs from pkl files
    files = [e.path for e in os.scandir(
        os.path.join(c.RESULT_FOLDER, c.GRAPHS)) if e.is_file() and e.name.endswith('.pkl')]
    graphs = c.read_result_object(files[0]) # [tracedataList, edgeList]
    # print(f"graph num in graphs:\n{len(graphs)}")
    # print(f"first graph in graphs:\n{graphs[0]}")

    # embedding: try 10 first
    nx_graphs = [nx.DiGraph(edgelist[1]) for edgelist in graphs[:11]]
    graph2vec = Graph2Vec(dimensions=20, wl_iterations=2, min_count=5, epochs=10)
    graph2vec.fit(nx_graphs)
    embeddings = graph2vec.get_embedding()
    print(f"embeddings for list nx_graphs: \n{embeddings}")

    # construct matrix
    mat = embeddings

    # PCA
    pca = PCA(n_components=2, svd_solver='arpack')
    X = pca.fit(mat).transform(mat)
    # Percentage of variance explained for each components
    print(
        "explained variance ratio (first two components): %s"
        % {str(pca.explained_variance_ratio_)}
    )
    print(f"output of pca:\n{X}")
    


if __name__ == '__main__':
    main()
