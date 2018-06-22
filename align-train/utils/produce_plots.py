#!/usr/bin/env python3
from math import ceil, sqrt

import matplotlib.pyplot as plt
import argparse
import re
from os import listdir
from os import path

from evaluation.common import draw_f1_lines


class Sample:
    def __init__(self, line):
        line = line.split('\t')
        self.p = float(line[1])
        self.r = float(line[2])
        self.f1 = float(line[3])
        self.t1 = float(line[4])
        self.t2 = float(line[5])


def main(input: list, output: str):
    # Parsing files
    files = dict()
    plots = 0
    min_prec = 1.0
    for file in input:
        files[file] = dict()
        cur_section = None
        cur_subsection = None
        with open(file, 'r') as reader:
            print("Parsing file %s" % file)
            for line in reader:
                line = line.rstrip()
                if ':' in line:
                    cur_section = line.rstrip(':')
                    cur_subsection = 'All'
                    files[file][cur_section] = dict()
                    continue
                if '\t' not in line:
                    cur_subsection = line
                    continue

                sample = Sample(line)
                if sample.p < min_prec:
                    min_prec = sample.p
                #if sample.t1 != 0.4 or cur_subsection != 'All' or cur_section != 'Selection eval':
                #    continue
                subsection_name = "%s %.1f min improvement" % (cur_subsection, sample.t1)
                if subsection_name not in files[file][cur_section]:
                    plots += 1
                    files[file][cur_section][subsection_name] = []
                files[file][cur_section][subsection_name].append(sample)
    min_prec = 0.65

    # Producing joint graphs
    file1 = files[input[0]]
    plots //= len(files)
    grid_size = int(ceil(sqrt(plots)))
    saturation = int(ceil(plots / grid_size))
    print("Plots: %d Grid size: %d Saturation: %d" % (plots, grid_size, saturation))
    fig = plt.figure(figsize=(7*grid_size, 7*saturation))
    plot = 0
    for section in file1:
        for subsection in file1[section]:
            plot += 1
            sp = fig.add_subplot(grid_size, grid_size, plot)
            sp.title.set_text("%s %s" % (section, subsection))
            draw_f1_lines(sp, prange=(min_prec, 1))

            sp.set_xlabel("Recall")
            sp.set_ylabel("Precision")

            sp.set_xlim(left=0, right=1)
            sp.set_ylim(bottom=min_prec, top=1)

            #markers = ['o', 's', '^', 'p']
            linestyles = ['-', '--', '-.', ':']
            cur_file = 0
            for file in files:
                label = re.match('(.+/)?([+@a-zA-Z0-9_-]+)\.[a-z]+$', file).group(2)
                x = []
                y = []
                if section not in files[file] or subsection not in files[file][section]:
                    continue
                source = sorted(files[file][section][subsection], key=lambda sample : sample.r)
                for sample in source:
                    x.append(sample.r)
                    y.append(sample.p)
                sp.plot(x, y, '-',
                        label=label, #marker=markers[cur_file % len(markers)],
                        markersize=2, linewidth=1)#, linestyle=linestyles[cur_file % len(linestyles)])
                cur_file += 1
            sp.legend(numpoints=1, loc='best')
    fig.tight_layout()
    fig.savefig(output)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plots precision/recall curves')
    parser.add_argument('--input', metavar='#', help='files from evaluation pipelines')
    parser.add_argument('--output', default=None, metavar='#', help='an output plot file')
    args = parser.parse_args()

    if args.output is None:
        args.output = path.join(args.input, 'output.pdf')
    main([path.join(args.input, file) for file in listdir(args.input) if file.endswith(".txt")], args.output)
