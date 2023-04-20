import misc as c
from collections import defaultdict
import collect_traces as ct
from pathlib import Path
import ast


def main():
    # trace = ast.literal_eval(Path(
    #     "../results/errors/traces_with_missing_rpc_level.txt").read_text().splitlines()[1])
    # print(trace)

    trace = [
        c.make_blank_row('0'),
        c.make_blank_row('0.1'),
        c.make_blank_row('0.1.1.1'),
        c.make_blank_row('0.2.1'),
    ]
    c.nice_display(trace)
    ct.fill_levels(trace)
    c.nice_display(trace)


if __name__ == '__main__':
    main()
