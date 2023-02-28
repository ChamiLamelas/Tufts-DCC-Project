import analysis as a
import os
from pathlib import Path

PATH = os.path.join("..", "data", "from_darby", "MSCallGraph_0.csv")
assert os.path.isfile(PATH), f"data file should be here = {PATH}"
RESULTS = os.path.join("..", "results")
Path(RESULTS).mkdir(parents=True, exist_ok=True)

g = a.Graphs(PATH)
a.plot_histogram(g, ["Called By", "Calling"], [
    os.path.join(RESULTS, "called_by_hist.png"),
    os.path.join(RESULTS, "calling_hist.png")
])
a.plot_degree_matrix(g, [
    os.path.join(RESULTS, "called_by_degree_matrix.png"),
    os.path.join(RESULTS, "calling_degree_matrix.png")
])
a.calculate_sparsity_ratio(g, os.path.join(RESULTS, "sparsity_ratio.txt"))
a.calculate_called_by1(g, os.path.join(RESULTS, "called_by1.txt"))