import os
import misc as c
import collect_traces as ct


def per_file_func(f):
    return [ct.get_max_concurrency_and_depth(trace) for (trace, _) in c.read_result_object(f)]


def main():
    files = [e.path for e in os.scandir(
        os.path.join(c.RESULT_FOLDER, c.GRAPHS)) if e.is_file() and e.name.endswith('.pkl')]
    results = c.run_func_on_data_files(per_file_func, fileset=c.get_idxs(), files=files)
    


if __name__ == '__main__':
    main()
