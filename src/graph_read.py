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

def main():
    # get list of graphs from pkl files
    files = [e for e in os.scandir(os.path.join(c.RESULT_FOLDER, c.GRAPHS_V2)) if e.is_file() and e.name.endswith('.pkl')]
    
    # loop over each pkl file as graphs
    # num_files = 1
    # services = defaultdict(list)
    i = 0
    for f in files:
        graphs = c.read_result_object(f.path) # [tracedataList, edgeList, edgefeatures]
        if graphs[0]: # just to see if anything's nonempty
            print(graphs[0])
    # for trace, edgelist, edgefeatrs in graphs:
    #     print(f"graph {i} in file {files[0].name}:")
    #     print(trace)
    #     print(edgelist)
    #     print(edgefeatrs)
