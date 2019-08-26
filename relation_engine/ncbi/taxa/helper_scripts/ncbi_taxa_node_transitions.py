#!/usr/bin/env python

# Calculates the count of each node transition from state to state
# (does not exist, exists, deleted, merged).
# Use -h for help.

import argparse
import hashlib
import json
import pathlib
import re
import texttable
from collections import defaultdict
 
ST_NO_EXIST = '∅'
ST_EXIST = 'E'
ST_DELETE = 'D'
ST_MERGED = 'M'
ARROW = '→'

NAMES_IN_FILE = 'names.dmp'
NODES_IN_FILE = 'nodes.dmp'
DEL_IN_FILE = 'delnodes.dmp'
MERGED_IN_FILE = 'merged.dmp'

NODES = 'nodes'
DELETED = 'deleted'
MERGED = 'merged'
LINE_MD5S = 'MD5S'

SEP = r'\s\|\s?'
SCI_NAME = 'scientific name'

KEY_TO_FILE = {NODES: NODES_IN_FILE,
               DELETED: DEL_IN_FILE,
               MERGED: MERGED_IN_FILE
               }

KEY_TO_STATE = {NODES: ST_EXIST,
                DELETED: ST_DELETE,
                MERGED: ST_MERGED
                }

# TODO CODE Update this to use common code when it works

def load_names(name_file):
    # Could make this use less memory by parsing one nodes worth of entries at a time, since
    # both the names and nodes files are sorted by taxid. YAGNI for now
    name_table = defaultdict(lambda: defaultdict(list))
    with open(name_file) as nf:
        for line in nf:
            tax_id, name, _, category = re.split(SEP, line)[0:4]
            name_table[tax_id.strip()][category.strip()].append(name.strip())

    return {k: dict(name_table[k]) for k in name_table.keys()}

def make_node_md5(node_line, names):
    # should really make the ints constants but meh
    id_, rank, gencode = [node_line[i].strip() for i in [0,2,6]]

    aliases = []
    # May need to move names into separate nodes for canonical search purposes
    for cat in list(names[id_].keys()):
        if cat != SCI_NAME:
            for nam in names[id_][cat]:
                aliases.append({'category':  cat, 
                                'name':      nam, 
                                })

    # vertex
    sci_names = names[id_][SCI_NAME]
    if len(sci_names) != 1:
        raise ValueError('Node {} has {} scientific names'.format(id_, len(sci_names)))
    node = {
            'id':                         id_,
            'scientific_name':            sci_names[0],
            'rank':                       rank,
            'aliases':                    aliases,
            'ncbi_taxon_id':              int(id_),
            'gencode':                    gencode,
            }
    jstr = json.dumps(node, sort_keys=True)
    return hashlib.md5(jstr.encode('UTF-8')).hexdigest()

def load_nodestates(taxdir):
    names = load_names(taxdir / NAMES_IN_FILE)
    parents = {}
    ret = defaultdict(set)
    md5s = dict()
    for key, f in KEY_TO_FILE.items():
        with open(taxdir / f) as taxfile:
            for l in taxfile:
                record = re.split(SEP, l)
                id_ = record[0].strip()
                ret[key].add(id_)
                if key == NODES:
                    parents[id_] = record[1].strip()
                    md5s[id_] = make_node_md5(record, names)
    return dict(ret), md5s, parents

def get_state(nodeid, nodestates):
    for key, state in KEY_TO_STATE.items():
        if nodeid in nodestates[key]:
            return state
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
    last_dir = dirs.pop(0)
    last_nodestates, last_md5s, last_parents = load_nodestates(p / last_dir)
    sizes = {last_dir: {'n': len(last_nodestates[NODES]),
                        'd': len(last_nodestates[DELETED]),
                        'm': len(last_nodestates[MERGED])
                        }
             }
    mismatches = {}
    parent_changes = {}
    for d in dirs:
        print(f'Processing {d}')
        nodestates, md5s, parents = load_nodestates(p / d)
        mismatches[d] = 0
        for id_ in md5s.keys():
            if id_ in last_md5s and last_md5s[id_] != md5s[id_]:
                mismatches[d] += 1
        parent_changes[d] = 0
        for id_ in parents.keys():
            if id_ in last_parents and last_parents[id_] != parents[id_]:
                parent_changes[d] += 1
        sizes[d] = {'n': len(nodestates[NODES]),
                    'd': len(nodestates[DELETED]),
                    'm': len(nodestates[MERGED])
                    }
        for key, state in KEY_TO_STATE.items():
            for nodeid in nodestates[key]:
                laststate = get_state(nodeid, last_nodestates)
                transitions[laststate + ARROW + state] += 1
        last_nodestates = nodestates
        last_md5s = md5s
        last_parents = parents
    
    s = sum(transitions.values())

    tt = texttable.Texttable()
    tt.set_deco(0)
    tt.add_row(['Transition', 'Count', 'Percent'])
    for k, v in sorted(transitions.items(), key=lambda kv: kv[1], reverse=True):
        tt.add_row([k, v, f'{(v / s) * 100:.2f}%'])
    print(tt.draw())
    print()

    tt = texttable.Texttable()
    tt.set_deco(0)
    tt.add_row(['Tax dump', 'Nodes', 'Deleted', 'Merged'])
    for k in sorted(sizes.keys()):
        tt.add_row([k, sizes[k]['n'], sizes[k]['d'], sizes[k]['m']])
    print(tt.draw())
    print()

    tt = texttable.Texttable()
    tt.set_deco(0)
    tt.add_row(['Tax dump', 'Node changes'])
    for k in sorted(mismatches.keys()):
        tt.add_row([k, mismatches[k]])
    print(tt.draw())
    print()

    tt = texttable.Texttable()
    tt.set_deco(0)
    tt.add_row(['Tax dump', 'Node parent changes'])
    for k in sorted(parent_changes.keys()):
        tt.add_row([k, parent_changes[k]])
    print(tt.draw())

if __name__ == '__main__':
    main()