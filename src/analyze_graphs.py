# for reading pkl file graphs
import misc as c
import os
from collections import defaultdict

# for embedding
from collect_traces import find_roots # aggregate traces by the root service
import networkx as nx
from karateclub import Graph2Vec
# for PAC
from sklearn.decomposition import PCA
import numpy as np
# for storing and plotting
import json

# graphs = [traceList, edgeList]
def generate_embeddings(graphs, embedding_size=20):
    nx_graphs = [nx.DiGraph(edgelist) for edgelist in graphs]
    graph2vec = Graph2Vec(dimensions=20, wl_iterations=2, min_count=2, epochs=10)
    graph2vec.fit(nx_graphs)
    embeddings = graph2vec.get_embedding()
    return embeddings


def main():
    # get list of graphs from pkl files
    files = [e for e in os.scandir(
        os.path.join(c.RESULT_FOLDER, c.GRAPHS)) if e.is_file() and e.name.endswith('.pkl')]
    
    # loop over each pkl file as graphs
    graphs = c.read_result_object(files[0].path) # [tracedataList, edgeList]
    services = defaultdict(list)
    for trace, edgelist in graphs:
        root = trace[0][0]
        if root == -1 or len(find_roots(trace))>1:
            continue

        # unique service root
        services[root].append(edgelist)
    # for k in services:
    #     print(f"service id: {k}, #call graphs: {len(services[k])}")

    # construct embedding matrix for each service
    embeddings = defaultdict(list)
    for s in services:
        if len(services[s]) < 2: continue # drop services with only 1 call graph
        embeddings[s] = generate_embeddings(services[s])

    # PCA
    pca = PCA(n_components=2)
    eigenVecs = {s: pca.fit(embeddings[s]).transform(embeddings[s]).T.tolist() for s in embeddings}

    # overview of the workflow
    print(f"Within {files[0].name}:\n\tnum of root services:{len(embeddings)}")
    for s in embeddings:
        print(f"\tservice id: {s}, eigen vector shape: {len(eigenVecs[s])}x{len(eigenVecs[s][0])}")

    # save results as a json file
    embed_path = os.path.join(c.RESULT_FOLDER, c.EMBEDDINGS)
    with open(os.path.join(embed_path, files[0].name[:-4]+'.json'), 'w') as f:
        json.dump(eigenVecs, f, indent=4)
    print(f"Saving to {os.path.join(embed_path, files[0].name[:-4]+'.json')}")
    # raise Exception("Breakpoint!")


if __name__ == '__main__':
    main()
