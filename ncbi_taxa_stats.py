#! /usr/bin/env python

# Calulates, for an uncompressed NCBI taxonomy dump:
# mean, median, stddev, max number of children per parent
# same for number of names per node
# Verifies name and nodes files are sorted by taxon ID
# 
# No online median algorithm I'm aware of, so we have to keep the list of data in memory

# Arguments - the directory containing the uncompressed NCBI taxon data.

import argparse
from collections import defaultdict
import os
import re
import sys
import numpy as np
import statistics as stats
import texttable

NAMES_FILE = 'names.dmp'
NODES_FILE = 'nodes.dmp'
TAXID_IDX = 0
NODES_PARENT_IDX = 1
SEP = r'\s\|\s?'

def extract_column_frequencies(path, column_index, init_with_first_column=False):
    d = defaultdict(int)
    with open(path) as f:
        last_col = -1
        for l in f:
            l = re.split(SEP, l)
            col1_str = l[0].strip()
            col1 = int(col1_str)
            if init_with_first_column:
                d[col1_str] # init to 0 if not already in dict
            if last_col > col1:
                raise ValueError('Found unsorted file {} at tax id {}'.format(path, col1))
            last_col = col1
            d[l[column_index].strip()] += 1
    return dict(d)

# returns mean, median, max, pop stddev
def calculate_stats(vals):
    # crappy efficiency here but meh
    mean = stats.mean(vals)
    mx = max(vals)
    hist_bins = []
    hist_bins_str = []
    bn = 1
    while bn < mx:
        bn = bn * 10
        hist_bins.append(bn + 1) # NP bins are non-inclusive on the right
        hist_bins_str.append(str(bn + 1) + '-' + str(bn * 10))
    hist_bins = [0, 1, 2, 3] + hist_bins
    hist_bins_str = ['0', '1', '2', '3-10'] + hist_bins_str
    hist = np.histogram(list(vals), hist_bins)[0]
    return {'mean': mean,
            'median': stats.median(vals),
            'max': mx,
            'stddev': stats.pstdev(vals, mean),
            'hist': hist,
            'hist_bins': hist_bins_str
            }

def print_stats(datatype, mean, median, max, stddev, hist, hist_bins):
    print('{}: Mean: {} Median: {} Max: {} Stddev: {}'.format(datatype, mean, median, max, stddev))
    tt = texttable.Texttable()
    tt.set_deco(0)
    tt.add_row([datatype, 'Node count'])
    for h, b in zip(hist, hist_bins):
        tt.add_row([b, h])
    print(tt.draw())
    print()


def parseargs():
    parser = argparse.ArgumentParser(description='Calculate statistics on an NCBI taxonomy dump.')
    parser.add_argument('--dir', required=True, help='the directory containing the dump file')

    return parser.parse_args()

def main():
    a = parseargs()

    names_per_taxa = extract_column_frequencies(os.path.join(a.dir, NAMES_FILE), TAXID_IDX)
    print_stats('Names per taxa', **calculate_stats(names_per_taxa.values()))
    del names_per_taxa

    children_per_taxa = extract_column_frequencies(
        os.path.join(a.dir, NODES_FILE), NODES_PARENT_IDX, True)
    print_stats('Children per taxa', **calculate_stats(children_per_taxa.values()))

if __name__ == '__main__':
    main()