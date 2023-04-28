import build_call_graph as bcg
import collect_traces as ct
import misc as c
import os


def per_file_func(file, prereqs):
    result = list()
    for _, trace in ct.get_trace_data(file, *prereqs).items():
        graph = bcg.build_call_graph_no_ms(trace)
        if graph is not None:
            result.append(graph)
    c.save_result_object(os.path.join(c.GRAPHS_V2, file + '.graphs.pkl'), result)


def main():
    c.run_func_on_data_files(
        per_file_func, ct.collect_prerequisites(), fileset=c.get_idxs(), ignore_result=True)


if __name__ == '__main__':
    main()
