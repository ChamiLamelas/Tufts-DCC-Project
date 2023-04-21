# Alibaba Trace Analysis

## Hardware

CloudLab c220g5

## Dataset

[github](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2021)

## Instructions 

* Run `build_aggregate_dependencies.py`
    * This produces the called by, calling graphs, unique microservices, and numer of traces each microservice appears in for all the data files.
    * This takes around 30 minutes to run on CloudLab. 
* Then, run `aggregate_dependency_analysis.py`
    * This produces called by, calling distributions, summary statistics such as the sparsity ratio, connected component sizes, and more building off the output of above. 
    * This takes around 4 minutes to run on CloudLab.
* Then, run `trace_contiguity_analysis.py` 
    * This produces the files on which each trace occurs as well as some information about the (lack of) contiguity of traces in the dataset. 
    * This takes around 10 minutes to run on CloudLab.
* Then, run `trace_analysis.py`
    * This produces statistics on errors in the trace files that were collected by above file.
    * This takes 15 minutes to run on CloudLab.
* Then, run `trace_plots.py` 
    * This produces plots using some of collected statistics from above.
    * This takes around 3 minutes to run on CloudLab.
* Run `get_nice_traces.py x` to get `x` instances of nice traces (RPC IDs are unique). May have other issues.
* Run `sample_error_traces.py x` to get `x` instances of traces with not unique RPC IDs, `x` traces missing 1 microservice ID, and `x` traces missing 2 microservice IDs.
* Then, run `even_more_trace_analysis.py`
    * This produces statistics on errors in the trace files that were collected by above file.
    * This takes 15 minutes to run on CloudLab.

Results files: 
* Generally, `.pkl` files have some objects we construct in an expensive job (e.g. aggregate dependency graphs). 
* `.png` are plots.
* `.txt` hold statistic results (check `errors/` subdirectory for those specifically pertaining to oddities in the data).

Utilities files: 
* `misc.py`, `collect_traces.py` and `build_call_graph.py` are all modules with a variety of utilities used by other files.