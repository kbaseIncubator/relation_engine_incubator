#! /usr/bin/env python

# Authors: Sean McCorkle, Gavin Price
# 
# This script transforms an NCBI taxa dump into ArangoDB (ADB) JSON bulk load format.

# It should only be used for an initial load into the DB; delta loads for subsequent tax dumps
# should be handled by the delta load script.

# The script requires three inputs:
# 1) The directory containing the unzipped taxa dump
# 2) The name of the ADB collection in which the taxa nodes will be loaded
#    (this is used to create the _from and _to fields in the edges)
# 3) The version of the load - this is also expected to be unique between this base load and
#    any delta loads.
# 4) The time stamp for the load in unix epoch milliseconds - all nodes and edges will be marked
#    with this timestamp as the creation date.

# The script creates three JSON files for uploading into Arango:   
#       ncbi_taxa_nodes.json    - the taxa vertexes
#       ncbi_taxa_edges.json    - the taxa edges

# TODO TESTS
# TODO quite a bit of this code can be shared with the delta loader

import argparse
import json
import os
import re
import sys
import unicodedata
from collections import defaultdict
from pprint import pprint

NODES_OUT_FILE = 'ncbi_taxa_nodes.json'
EDGES_OUT_FILE = 'ncbi_taxa_edges.json'

NAMES_IN_FILE = 'names.dmp'
NODES_IN_FILE = 'nodes.dmp'

SCI_NAME = 'scientific name'

CANONICAL_IGNORE_SET = {'et','al','and','or','the','a'}

SEP = r'\s\|\s?'

# see https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/
# in unix epoch ms this is 2255/6/5
MAX_ADB_INTEGER = 2**53 - 1

def load_names(name_file):
    # Could make this use less memory by parsing one nodes worth of entries at a time, since
    # both the names and nodes files are sorted by taxid. YAGNI for now
    name_table = defaultdict(lambda: defaultdict(list))
    with open(name_file) as nf:
        for line in nf:
            tax_id, name, _, category = re.split(SEP, line)[0:4]
            name_table[tax_id.strip()][category.strip()].append(name.strip())

    return {k: dict(name_table[k]) for k in name_table.keys()}


# assumes there's at least one non-whitespace char in string
def canonicalize(string, ignore_tokens):
    # see https://docs.python.org/3/howto/unicode.html#comparing-strings
    normed = unicodedata.normalize('NFD', unicodedata.normalize('NFD', string).casefold())
    # maybe include the full string, but normed, in the returned list?
    tokens = normed.split()
    # TODO TEST for fencepost errors here
    ret = []
    for t in tokens:
        for start in range(len(t)):
            if t[start].isalpha() or t[start].isdigit():
                break
        for end in range(len(t) - 1, -1, -1):
            if t[end].isalpha() or t[end].isdigit():
                break
        if start <= end:
            t = t[start: end + 1]
            if t not in ignore_tokens:
                ret.append(t)
    return ret

def process_nodes(
        nodes_in,
        name_table,
        node_collection,
        load_version,
        timestamp,
        nodes_out,
        edges_out
        ):
    with open(nodes_in) as infile, open(nodes_out, 'w') as nodef, open(edges_out, 'w') as edgef:
        for line in infile:
            record = re.split(SEP, line)
            # should really make the ints constants but meh
            id_, parent, rank, gencode = [record[i].strip() for i in [0,1,2,6]]
            node_id = id_ + '_' + load_version

            aliases = []
            # May need to move names into separate nodes for canonical search purposes
            for cat in list(name_table[id_].keys()):
                if cat != SCI_NAME:
                    for nam in name_table[id_][cat]:
                        aliases.append({'category':  cat, 
                                        'name':      nam, 
                                        'canonical': canonicalize(nam, CANONICAL_IGNORE_SET)
                                        })

            # vertex
            sci_names = name_table[id_][SCI_NAME]
            if len(sci_names) != 1:
                raise ValueError('Node {} has {} scientific names'.format(id_, len(sci_names)))
            nodef.write(json.dumps(
                {'_key':                       node_id,
                 'id':                         id_,
                 'scientific_name':            sci_names[0],
                 'canonical_scientific_name':  canonicalize(sci_names[0], CANONICAL_IGNORE_SET),
                 'rank':                       rank,
                 'aliases':                    aliases,
                 'ncbi_taxon_id':              int(id_),
                 'gencode':                    gencode,
                 'first_version':              load_version,
                 'last_version':               load_version,
                 'created':                    timestamp,
                 'expires':                    MAX_ADB_INTEGER
                 }
                ) + '\n')
            
            # edge
            if id_ != parent:  # no self edges
                edgef.write(json.dumps(
                    {'_from':            node_collection + '/' + node_id,
                     'from':             node_collection + '/' + id_,
                     '_to':              node_collection + '/' + parent + '_' + load_version,
                     'to':               node_collection + '/' + parent,
                     'first_version':    load_version,
                     'last_version':     load_version,
                     'created':          timestamp,
                     'expires':          MAX_ADB_INTEGER,
                     'type':             'std'                 # as opposed to merge
                     }
                    ) + '\n')

def parse_args():
    parser = argparse.ArgumentParser(
        description='Create ArangoDB bulk load files from an NCBI taxa dump.')
    parser.add_argument('--dir', required=True,
                        help='the directory containing the unzipped dump files')
    parser.add_argument(
        '--node-collection',
        required=True,
        help='the name of the ArangoDB collection into which taxa nodes will be loaded')
    parser.add_argument(
        '--load-version',
        required=True,
        help='the version of this load. This version will be added to a field in the nodes and ' +
             'edges and will be used as part of the _key field for nodes.')
    parser.add_argument(
        '--load-timestamp',
        type=int,
        required=True,
        help='the timestamp to be applied to the load, in unix epoch milliseconds. Any nodes ' +
             'or edges created in this load will start to exist with this time stamp. ')

    return parser.parse_args()

def main():
    a = parse_args()

    name_table = load_names(os.path.join(a.dir, NAMES_IN_FILE))

    process_nodes(
        os.path.join(a.dir, NODES_IN_FILE),
        name_table,
        a.node_collection,
        a.load_version,
        a.load_timestamp,
        os.path.join(a.dir, NODES_OUT_FILE),
        os.path.join(a.dir, EDGES_OUT_FILE))

if __name__  == '__main__':
    main()
