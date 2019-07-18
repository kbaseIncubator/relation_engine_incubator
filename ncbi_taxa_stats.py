#! /usr/bin/env python

# Calulates, for an uncompressed NCBI taxonomy dump:
# mean, median, stddev, max number of children per parent
# same for number of names per node
# Verifies name and nodes files are sorted by taxon ID
# 
# No online median algorithm I'm aware of, so we have to keep the list of data in memory

# Arguments - the directory containing the uncompressed NCBI taxon data.

import os
import re
import sys
import statistics as stats
from collections import defaultdict

NAMES_FILE = 'names.dmp'
NODES_FILE = 'nodes.dmp'
TAXID_IDX = 0
NODES_PARENT_IDX = 1
SEP = r'\s\|\s?'

# returns mean, median, max, pop stddev
def calculate_stats(path, feature_index):
    d = defaultdict(int)
    with open(path) as f:
        last_tax = -1
        for l in f:
            l = re.split(SEP, l)
            tax_id = int(l[TAXID_IDX])
            if last_tax > tax_id:
                raise ValueError('Found unsorted file {} at tax id {}'.format(path, tax_id))
            last_tax = tax_id
            d[l[feature_index]] += 1
    # crappy efficiency here but meh
    mean = stats.mean(d.values())
    return mean, stats.median(d.values()), max(d.values()), stats.pstdev(d.values(), mean)

def print_stats(datatype, mean, median, max, stddev):
    print('{}: Mean: {} Median: {} Max: {} Stddev: {}'.format(datatype, mean, median, max, stddev))

def main():
    if len(sys.argv) < 2:
        sys.exit('Must specify directory with NCBI taxa files')
    directory = sys.argv[1]

    print_stats('Names per taxa', *calculate_stats(os.path.join(directory, NAMES_FILE), TAXID_IDX))
    print_stats('Children per taxa',
        *calculate_stats(os.path.join(directory, NODES_FILE), NODES_PARENT_IDX))

if __name__ == '__main__':
    main()