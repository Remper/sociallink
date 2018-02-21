import argparse

from os import path, listdir


class Sample:
    def __init__(self, line):
        line = line.split('\t')
        self.name = line[0]
        self.p = float(line[1])
        self.r = float(line[2])
        self.f1 = float(line[3])
        self.t1 = float(line[4])
        self.t2 = float(line[5])
        self.amount = 1

    def __str__(self):
        return "%s_%.2f_%.2f" % (self.name, self.t1, self.t2)

    def add(self, other):
        assert self.name == other.name and self.t1 == other.t1 and self.t2 == other.t2
        self.p += other.p
        self.r += other.r
        self.f1 += other.f1
        self.amount += other.amount

    def normalize(self):
        if self.amount <= 1:
            return
        self.p /= self.amount
        self.r /= self.amount
        self.f1 /= self.amount
        self.amount = 1


def main(work_dir: str, prefix: str):
    input_files = [path.join(work_dir, file) for file in listdir(work_dir) if file.endswith(".txt") and file.startswith(prefix+'_')]
    files = dict()
    for file in input_files:
        cur_section = None
        cur_subsection = None
        with open(file, 'r') as reader:
            print("Parsing file %s" % file)
            for line in reader:
                line = line.rstrip()
                if ':' in line:
                    cur_section = line.rstrip(':')
                    cur_subsection = 'All'
                    continue
                if '\t' not in line:
                    cur_subsection = line
                    continue

                sample = Sample(line)
                if cur_section not in files:
                    files[cur_section] = dict()
                if cur_subsection not in files[cur_section]:
                    files[cur_section][cur_subsection] = dict()

                if str(sample) in files[cur_section][cur_subsection]:
                    files[cur_section][cur_subsection][str(sample)].add(sample)
                else:
                    files[cur_section][cur_subsection][str(sample)] = sample

    with open(path.join(work_dir, prefix+'_out.txt'), 'w') as writer:
        first = True
        for section_name in files:
            cur_section = files[section_name]
            if not first:
                writer.write('\n')
            first = False
            writer.write(section_name+':')
            for subsection_name in cur_section:
                cur_subsection = cur_section[subsection_name]
                writer.write('\n')
                writer.write(subsection_name)
                for cur_sample in sorted([value for value in cur_subsection.values()], key=lambda v: str(v)):
                    amount = cur_sample.amount
                    cur_sample.normalize()
                    writer.write('\n')
                    writer.write('%s\t%.4f\t%.4f\t%.4f\t%.1f\t%.2f\t%d' % (
                        cur_sample.name, cur_sample.p, cur_sample.r,
                        cur_sample.f1, cur_sample.t1, cur_sample.t2, amount
                    ))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Plots precision/recall curves')
    parser.add_argument('--workdir', metavar='#', help='workdir')
    parser.add_argument('--prefix', metavar='#', help='prefix for the files')
    args = parser.parse_args()

    main(args.workdir, args.prefix)
