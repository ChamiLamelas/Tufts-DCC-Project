import pickle
import os

with open(os.path.join("..", "results", "project_update_2", "call_graph.pickle"), 'rb') as handle:
    graph = pickle.load(handle)

# Convert into pyg.Data here 
# will need to map rpcid keys in the returned graph to numbers to construct edge_index
# x (features) can just be all set to 1 i think