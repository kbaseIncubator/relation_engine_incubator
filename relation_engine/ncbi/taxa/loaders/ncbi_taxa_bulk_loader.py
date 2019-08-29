#! /usr/bin/env python

# Authors: Sean McCorkle, Gavin Price
# 
# This script transforms an NCBI taxa dump into ArangoDB (ADB) JSON bulk load format.

# It should only be used for an initial load into the DB; delta loads for subsequent tax dumps
# should be handled by the delta load script.

# The script requires three inputs:
# 1) The directory containing the unzipped taxa dump
# 2) The version of the load - this is also expected to be unique between this base load and
#    any delta loads.
# 3) The time stamp for the load in unix epoch milliseconds - all nodes and edges will be marked
#    with this timestamp as the creation date.

# The script creates two JSON files for uploading into Arango:   
#       ncbi_taxa_nodes.json    - the taxa vertexes
#       ncbi_taxa_edges.json    - the taxa edges

# TODO TESTS

import argparse
import os
import unicodedata

from relation_engine.batchload.load_utils import process_nodes
from relation_engine.batchload.load_utils import process_edges
from relation_engine.ncbi.taxa.parsers import NCBINodeProvider
from relation_engine.ncbi.taxa.parsers import NCBIEdgeProvider

NODES_OUT_FILE = 'ncbi_taxa_nodes.json'
EDGES_OUT_FILE = 'ncbi_taxa_edges.json'

NAMES_IN_FILE = 'names.dmp'
NODES_IN_FILE = 'nodes.dmp'

def parse_args():
    parser = argparse.ArgumentParser(
        description='Create ArangoDB bulk load files from an NCBI taxa dump.')
    parser.add_argument('--dir', required=True,
                        help='the directory containing the unzipped dump files')
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

    with open(nodes) as infile, open(edges_out, 'w') as edgef:
        edgeprov = NCBIEdgeProvider(infile)
        process_edges(edgeprov, a.load_version, a.load_timestamp, edgef)

if __name__  == '__main__':
    main()
