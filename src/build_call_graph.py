import misc as c

def main():
    nice_traces = c.read_object(c.NICE_TRACES_FILE)
    ids = c.read_result_object(c.TRACE_IDS_FILE)
    for tid, t in nice_traces.items():
        print(ids[tid], t, end='\n\n')


if __name__ == '__main__':
    main()