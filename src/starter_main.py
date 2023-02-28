import analysis as a
import os
from pathlib import Path

PATH = os.path.join("..", "data", "from_darby", "MSCallGraph_0.csv")
assert os.path.isfile(PATH), f"data file should be here = {PATH}"
PLOTS = os.path.join("..", "results")
Path(PLOTS).mkdir(parents=True, exist_ok=True)

g = a.Graphs(PATH)
a.plot_histogram(g, ["Called By", "Calling"], [
    os.path.join(PLOTS, "called_by_hist.png"),
    os.path.join(PLOTS, "calling_hist.png")
])
a.plot_degree_matrix(g, [
    os.path.join(PLOTS, "called_by_degree_matrix.png"),
    os.path.join(PLOTS, "calling_degree_matrix.png")
])
