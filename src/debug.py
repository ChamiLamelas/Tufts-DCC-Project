import misc as c
import collect_traces as ct

trace = [
    [-1, 3, '0.1', 'rpc', 1],
    [1, -1, '0', 'rpc', 1],
    [2, 3, '0.1', 'rpc', 1],
    [-1, 4, '0.1.1', 'rpc', 1],
]

c.nice_display(trace)
trace = c.make_hierarchy(trace)
c.nice_display(trace)
ct.patch_missing(trace)
c.nice_display(trace)

