import argparse
import codecs
import gzip
import json
from os import listdir, path


def fill_gold(gold, files):
    result = {}

    counter = 0
    for file in files:
        with gzip.open(file, 'rt') as reader:
            for line in reader:
                row = line.rstrip().split('\t')

                if len(row) < 2:
                    print("bad line: %s" % line)
                    continue

                uid = row[0]
                #obj = json.loads(codecs.getdecoder("unicode_escape")(row[1])[0])

                #name = obj["screen_name"].lower()
                name = row[1].lower()
                if name in gold:
                    result[name] = uid
                    gold.remove(name)

                if len(gold) == 0:
                    return result

                counter += 1
                if counter % 100000 == 0:
                    print("Processed %.1fm lines. Current file: %s. Dict size: %d. Left: %d" % (
                        counter / 1000000,
                        file.split('/')[-1],
                        len(result),
                        len(gold)
                    ))

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
    parser.add_argument('--output', metavar='#', help='folder for an output')
    args = parser.parse_args()

    #main(args.list, [path.join(args.objects, file) for file in listdir(args.objects) if file.startswith("objects")], args.output)
    main(args.list, [args.objects], args.output)