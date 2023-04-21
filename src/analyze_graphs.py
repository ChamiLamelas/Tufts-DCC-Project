import misc as c
import os


def main():
    files = [e.path for e in os.scandir(
        c.GRAPHS) if e.is_file() and e.name.endswith('.pkl')]
    graphs = c.read_result_object(files[0])
    print(graphs[0])


if __name__ == '__main__':
    main()
