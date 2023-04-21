import os
#

def countlines(filepath):
    with open(filepath, 'r') as f:
        return sum(1 for line in f if (len(line.strip()) > 0 and not line.startswith("#")))


def collect_counts(dirs, skipfiles):
    total = 0
    files = 0
    for d in dirs:
        print(d)
        for entry in os.scandir(d):
            if entry.name not in skipfiles and entry.name.endswith(".py"):
                count = countlines(entry.path)
                print(f"\t{entry.name}: {count}")
                total += count
                files += 1
    print(f"\n{total} lines over {files} files")


def main():
    collect_counts([os.path.join("..", "src"),
                   os.path.join("..", "setup")], ["debug.py"])


if __name__ == '__main__':
    main()
