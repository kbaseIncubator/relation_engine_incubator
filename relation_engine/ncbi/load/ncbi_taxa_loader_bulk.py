#! /usr/bin/env python

# Authors: Sean McCorkle, Gavin Price
# 
# This script transforms an NCBI taxa dump into ArangoDB (ADB) JSON bulk load format.

# It should only be used for an initial load into the DB; delta loads for subsequent tax dumps
# should be handled by the delta load script.

# The script requires four inputs:
# 1) The directory containing the unzipped taxa dump
# 2) The name of the ADB collection in which the taxa nodes will be loaded
#    (this is used to create the _from and _to fields in the edges)
# 3) The version of the load - this is also expected to be unique between this base load and
#    any delta loads.
# 4) The time stamp for the load in unix epoch milliseconds - all nodes and edges will be marked
#    with this timestamp as the creation date.

# The script creates two JSON files for uploading into Arango:   
#       ncbi_taxa_nodes.json    - the taxa vertexes
#       ncbi_taxa_edges.json    - the taxa edges

# TODO TESTS

import argparse
import json
import os
import re
import sys
import unicodedata
from collections import defaultdict
from pprint import pprint

# probably want to namespace this behind biokbase
from relation_engine.ncbi.taxa_parsers import NCBINodeProvider
from relation_engine.ncbi.taxa_parsers import NCBIEdgeProvider

NODES_OUT_FILE = 'ncbi_taxa_nodes.json'
EDGES_OUT_FILE = 'ncbi_taxa_edges.json'

NAMES_IN_FILE = 'names.dmp'
NODES_IN_FILE = 'nodes.dmp'

# see https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/
# in unix epoch ms this is 2255/6/5
MAX_ADB_INTEGER = 2**53 - 1

def process_nodes(
        nodes_in,
        names_in,
        load_version,
        timestamp,
        nodes_out):
    with open(nodes_in) as infile, open(names_in) as namesfile, open(nodes_out, 'w') as nodef:
        nodeprov = NCBINodeProvider(namesfile, infile)
        for n in nodeprov:
            n.update({
                '_key':           n['id'] + '_' + load_version,
                'first_version':  load_version,
                'last_version':   load_version,
                'created':        timestamp,
                'expires':        MAX_ADB_INTEGER
                })
            nodef.write(json.dumps(n) + '\n')

def process_edges(
        nodes_in,
        node_collection,
        load_version,
        timestamp,
        edges_out,
        ):
    with open(nodes_in) as infile, open(edges_out, 'w') as edgef:
        edgeprov = NCBIEdgeProvider(infile)
        for e in edgeprov:
            e.update({
                '_from':            node_collection + '/' + e['from'] + '_' + load_version,
                'from':             node_collection + '/' + e['from'],
                '_to':              node_collection + '/' + e['to'] + '_' + load_version,
                'to':               node_collection + '/' + e['to'],
                'first_version':    load_version,
                'last_version':     load_version,
                'created':          timestamp,
                'expires':          MAX_ADB_INTEGER,
                'type':             'std'                 # as opposed to merge
            })
            edgef.write(json.dumps(e) + '\n')

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

    process_nodes(
        os.path.join(a.dir, NODES_IN_FILE),
        os.path.join(a.dir, NAMES_IN_FILE),
        a.load_version,
        a.load_timestamp,
        os.path.join(a.dir, NODES_OUT_FILE))

    process_edges(
        os.path.join(a.dir, NODES_IN_FILE),
        a.node_collection,
        a.load_version,
        a.load_timestamp,
        os.path.join(a.dir, EDGES_OUT_FILE))

if __name__  == '__main__':
    main()
