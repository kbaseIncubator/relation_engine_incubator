#! /usr/bin/env python

# TEST SCRIPT - puts edges in 3 different collections for testing multiple edges

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

import argparse
import json
import os
import unicodedata

from contextlib import ExitStack
from relation_engine.batchload.load_utils import process_nodes
from relation_engine.batchload.load_utils import process_edges, process_edge
from relation_engine.ncbi.taxa.parsers import NCBINodeProvider
from relation_engine.ncbi.taxa.parsers import NCBIEdgeProvider

NODES_OUT_FILE = 'ncbi_taxa_nodes_e3_test.json'
EDGES_OUT_FILE = 'ncbi_taxa_edges_e3_test.json'

NAMES_IN_FILE = 'names.dmp'
NODES_IN_FILE = 'nodes.dmp'

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
    nodes = os.path.join(a.dir, NODES_IN_FILE)
    names = os.path.join(a.dir, NAMES_IN_FILE)

    nodes_out = os.path.join(a.dir, NODES_OUT_FILE)
    edges_out = os.path.join(a.dir, EDGES_OUT_FILE)

    with open(nodes) as infile, open(names) as namesfile, open(nodes_out, 'w') as node_out:
        nodeprov = NCBINodeProvider(namesfile, infile)
        process_nodes(nodeprov, a.load_version, a.load_timestamp, node_out)

    with ExitStack() as stack:
        infile = stack.enter_context(open(nodes))
        files = {
            1: stack.enter_context(open(edges_out + '_1', 'w')),
            2: stack.enter_context(open(edges_out + '_2', 'w')),
            3: stack.enter_context(open(edges_out + '_3', 'w'))
        }
        edgeprov = NCBIEdgeProvider(infile)
        for e in edgeprov:
            e = process_edge(e, a.node_collection, a.load_version, a.load_timestamp)
            fileno = int(e['id']) % 3 + 1
            files[fileno].write(json.dumps(e) + '\n')

if __name__  == '__main__':
    main()
