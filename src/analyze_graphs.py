import misc as c
import os


def main():
    files = [e.path for e in os.scandir(
        os.path.join(c.RESULT_FOLDER, c.GRAPHS)) if e.is_file() and e.name.endswith('.pkl')]
    graphs = c.read_result_object(files[0])
    print(len(graphs))
    print(graphs[0][1])

if __name__ == '__main__':
    main()
