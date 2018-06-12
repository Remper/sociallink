import argparse
import codecs
import gzip
import json
from os import listdir, path


def process_line(name, uid, file_name, gold, result, counter, report=100000):
    if name in gold:
        result[name] = uid
        gold.remove(name)

    if counter % report == 0:
        print("Processed %.1fm lines. Current file: %s. Dict size: %d. Left: %d" % (
            counter / 1000000,
            file_name.split('/')[-1],
            len(result),
            len(gold)
        ))


def fill_gold(gold, files):
    result = {}

    counter = 0
    dictionary, object_files = files
    with gzip.open(dictionary, 'rt') as reader:
        for line in reader:
            row = line.rstrip().split('\t')

            if len(row) < 2:
                print("  bad line: %s" % line.rstrip())
                continue

            uid = row[0]
            name = row[1].lower()

            counter += 1
            process_line(name, uid, dictionary, gold, result, counter, 3000000)

            if len(gold) == 0:
                break

    for file in object_files:
        if len(gold) == 0:
            break
        with open(file, 'r') as reader:
            for line in reader:
                row = line.rstrip().split('\t')

                if len(row) < 2:
                    print("  bad line: %s" % line.rstrip())
                    continue

                uid = row[0]
                obj = json.loads(codecs.getdecoder("unicode_escape")(row[1])[0])
                name = obj["screen_name"].lower()

                counter += 1
                process_line(name, uid, file, gold, result, counter)

                if len(gold) == 0:
                    break

    return result


def main(gold_file, files, output):
    gold = set()
    header = None
    with open(gold_file, 'r') as reader:
        for line in reader:
            if header is None:
                header = line
                continue
            twitter_id = line.rstrip().split(',')[1].lower()
            gold.add(twitter_id)

    result = fill_gold(gold, files)
    with open(output, 'w') as writer:
        for ele in result:
            writer.write(ele)
            writer.write('\t')
            writer.write(result[ele])
            writer.write('\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plots precision/recall curves')
    parser.add_argument('--list', metavar='#', help='files from evaluation pipelines')
    parser.add_argument('--objects', metavar='#', help='postgres dump of objects table')
    parser.add_argument('--dictionary', metavar='#', help='dictionary produced by some old script')
    parser.add_argument('--output', metavar='#', help='folder for an output')
    args = parser.parse_args()

    main(args.list, (args.dictionary, [path.join(args.objects, file) for file in listdir(args.objects) if file.startswith("objects")]), args.output)