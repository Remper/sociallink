import argparse
import random
from os import path
import os
import shutil


def main(work_dir: str):
    # Opening gold standard folder
    samples = []
    header = None
    gold_file = path.join(work_dir, "gold.csv")
    with open(gold_file, 'r') as reader:
        for line in reader:
            line = line.rstrip()
            if header is None:
                header = line
                continue
            samples.append(line)

    print("Amount of samples in the gold standard: %d" % len(samples))
    random.shuffle(samples)

    splits = 5
    writers = []
    for i in range(splits):
        split_dir = path.join(work_dir, 'fold%d' % i)
        os.mkdir(split_dir)
        shutil.copy2(gold_file, path.join(split_dir, 'gold.csv'))
        os.symlink(path.join(work_dir, "resolved.json"), path.join(split_dir, "resolved.json"))
        test_writer = open(path.join(split_dir, 'test.csv'), 'w')
        test_writer.write(header)
        train_writer = open(path.join(split_dir, 'train.csv'), 'w')
        train_writer.write(header)
        writers.append((i, train_writer, test_writer))

    cur_sample = 0
    for sample in samples:
        test_id = cur_sample % splits
        for id, train_writer, test_writer in writers:
            if test_id == id:
                writer = test_writer
            else:
                writer = train_writer
            writer.write('\n')
            writer.write(sample)
        cur_sample += 1

    for id, train_writer, test_writer in writers:
        train_writer.close()
        test_writer.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plots precision/recall curves')
    parser.add_argument('--input', metavar='#', help='files from evaluation pipelines')
    args = parser.parse_args()

    main(args.input)