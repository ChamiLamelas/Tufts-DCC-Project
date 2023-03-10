import analysis as a
import os

RESULTFOLDER = os.path.join("..", "results", "project_update_2")

g = a.Graphs()

print(f"Num microservices = {g.num_microservices()}")

print(f"Num dependencies = {g.num_dependencies()}")

a.plot_histogram(g, ["Called By Degree", "Calling Degree"], [os.path.join(
    RESULTFOLDER, "true_called_by.png"), os.path.join(RESULTFOLDER, "true_calling.png")], 200)

a.calculate_sparsity_ratio(g, os.path.join(
    RESULTFOLDER, "true_sparsity_ratio.txt"))

a.calculate_called_by1(g, os.path.join(RESULTFOLDER, "true_called_by1.txt"))

a.calculate_connected_components(g, os.path.join(
    RESULTFOLDER, "true_connected_components.txt"))

a.calculate_correlation(g, [os.path.join(RESULTFOLDER, "called_by_correlation.png"), os.path.join(
    RESULTFOLDER, "calling_correlation.png")])
