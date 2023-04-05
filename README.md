# Alibaba Trace Analysis

## Dataset

[github](https://github.com/alibaba/clusterdata/tree/master/cluster-trace-microservices-v2021)

## Instructions 

* Run `build_aggregate_dependencies.py`
    * This produces the called by, calling graphs, unique microservices, and numer of traces each microservice appears in for all the data files.
    * This takes around 30 minutes to run on CloudLab. 
* Then, run `aggregate_dependency_analysis.py`
    * This produces called by, calling distributions, summary statistics such as the sparsity ratio, connected component sizes, and more building off the output of above. 
    * This runs very quickly (1 minute) on CloudLab.
* Then, run `trace_contiguity_analysis.py` 
    * This produces the files on which each trace occurs as well as some information about the (lack of) contiguity of traces in the dataset. 
    * This takes around 10 minutes to run on CloudLab.
* Then, run `collect_traces.py` 
    * This produces trace files where now traces actually have been collected together and integerized the necessary information from their files. 
    * This involves some preprocessing: all traces that are split over multiple files (identified by above script) are dropped.
    * This takes around TODO minutes to run on CloudLab.
* Then, run `trace_analysis.py`
    * This produces statistics on errors in the trace files that were collected by above file.
    * This takes around TODO minutes to run on CloudLab.
* Then, run `trace_plots.py` 
    * This produces plots using some of collected statistics from above.
    * This takes around TODO minutes to run on CloudLab.

Results files: 
* Generally, `.pkl` files have some objects we construct in an expensive job (e.g. aggregate dependency graphs). 
* `.png` are plots.
* `.txt` hold statistic results (check `errors/` subdirectory for those specifically pertaining to oddities in the data).

Note: `misc.py` just has some various utilities used by the other files.