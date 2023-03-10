import pickle
import analysis as a
import os

# call graph is a dict - we'll pickle it and then save to file 

trace = None 

graph = a.get_graph(trace)

with open(os.path.join("..", "results", "project_update_2", "call_graph.pickle"), 'wb') as handle:
    pickle.dump(graph, handle, protocol=pickle.HIGHEST_PROTOCOL)
