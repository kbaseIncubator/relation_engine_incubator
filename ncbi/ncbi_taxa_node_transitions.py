#!/usr/bin/env python

# Calculates the count of each node transition from state to state
# (does not exist, exists, deleted, merged).
# Use -h for help.

import argparse
import pathlib
import re
from collections import defaultdict
 
ST_NO_EXIST = '∅'
ST_EXIST = 'E'
ST_DELETE = 'D'
ST_MERGED = 'M'
ARROW = '→'


NODES_IN_FILE = 'nodes.dmp'
DEL_IN_FILE = 'delnodes.dmp'
MERGED_IN_FILE = 'merged.dmp'

NODES = 'nodes'
DELETED = 'deleted'
MERGED = 'merged'

SEP = r'\s\|\s?'

KEY_TO_FILE = {NODES: NODES_IN_FILE,
               DELETED: DEL_IN_FILE,
               MERGED: MERGED_IN_FILE
               }

KEY_TO_STATE = {NODES: ST_EXIST,
                DELETED: ST_DELETE,
                MERGED: ST_MERGED
                }

def load_nodestates(taxdir):
    ret = defaultdict(set)
    for key, f in KEY_TO_FILE.items():
        with open(taxdir / f) as taxfile:
            for l in taxfile:
                id_ = re.split(SEP, l)[0].strip()
                ret[key].add(id_)
    return dict(ret)

def get_state(nodeid, nodestates):
    if nodeid in nodestates[NODES]:
        return ST_EXIST
    elif nodeid in nodestates[DELETED]:
        return ST_DELETE
    elif nodeid in nodestates[MERGED]:
        return ST_MERGED
    else:
        return ST_NO_EXIST

def parseargs():
    parser = argparse.ArgumentParser(
        description='Calculate node transition counts from a set of NCBI taxonomy dumps.')
    parser.add_argument('--dir', required=True,
                        help='the directory containing the tax files. Each set of tax dumps is ' +
                             'expected to be contained in its own directory, and those ' + 
                             'directories are expected to be named such that when sorted by ' +
                             'name the dumps are in temporal order.')

    return parser.parse_args()

def main():
    a = parseargs()

    p = pathlib.Path(a.dir)
    dirs = sorted([d for d in p.iterdir() if d.is_dir()])
    transitions = defaultdict(int)
    # last here means last iteration, not last in list
    last_nodestates = load_nodestates(p / dirs.pop(0))
    for d in dirs:
        nodestates = load_nodestates(p / d)
        for key, state in KEY_TO_STATE.items():
            for nodeid in nodestates[key]:
                laststate = get_state(nodeid, last_nodestates)
                transitions[laststate + ARROW + state] += 1
        last_nodestates = nodestates
    
    for k, v in sorted(transitions.items(), key=lambda kv: kv[1], reverse=True):
        print(f'{k} {v}')

if __name__ == '__main__':
    main()