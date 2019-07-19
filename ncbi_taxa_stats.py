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

# returns mean, median, max, pop stddev
def calculate_stats(path, feature_index):
    d = defaultdict(int)
    with open(path) as f:
        last_tax = -1
        for l in f:
            l = re.split(SEP, l)
            tax_id_str = l[TAXID_IDX].strip()
            tax_id = int(tax_id_str)
            d[tax_id_str] # init to 0 if not already in dict
            if last_tax > tax_id:
                raise ValueError('Found unsorted file {} at tax id {}'.format(path, tax_id))
            last_tax = tax_id
            d[l[feature_index].strip()] += 1
    # crappy efficiency here but meh
    mean = stats.mean(d.values())
    mx = max(d.values())
    hist_bins = []
    hist_bins_str = []
    bn = 1
    while bn < mx:
        bn = bn * 10
        hist_bins.append(bn + 1) # NP bins are non-inclusive on the right
        hist_bins_str.append(str(bn + 1) + '-' + str(bn * 10))
    hist_bins = [0, 1, 2, 3] + hist_bins
    hist_bins_str = ['0', '1', '2', '3-10'] + hist_bins_str
    hist = np.histogram(list(d.values()), hist_bins)[0]
    return {'mean': mean,
            'median': stats.median(d.values()),
            'max': mx,
            'stddev': stats.pstdev(d.values(), mean),
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

    print_stats('Names per taxa', **calculate_stats(os.path.join(a.dir, NAMES_FILE), TAXID_IDX))
    print_stats('Children per taxa',
        **calculate_stats(os.path.join(a.dir, NODES_FILE), NODES_PARENT_IDX))

if __name__ == '__main__':
    main()