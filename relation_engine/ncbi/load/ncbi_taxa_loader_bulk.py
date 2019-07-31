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

def process_nodes(nodeprov, load_version, timestamp, nodes_out):
    """
    Process graph nodes from a provider into a JSON load file for a batch time travelling load.

    This function is only suitable for the initial load in the time travelling database.
    Further loads must use a delta load algorithm.

    Nodes are expected to have an 'id' field containing the node's unique ID.

    nodeprov - the node provider. This is an iterable that returns nodes represented as dicts.
    load_version - the version of the load in which the nodes appear.
    timestamp - the timestamp at which the nodes will begin to exist.
    nodes_out - a handle to the file where the nodes will be written.
    """
    for n in nodeprov:
        n.update({
            '_key':           n['id'] + '_' + load_version,
            'first_version':  load_version,
            'last_version':   load_version,
            'created':        timestamp,
            'expires':        MAX_ADB_INTEGER
            })
        nodes_out.write(json.dumps(n) + '\n')

def process_edges(edgeprov, node_collection, load_version, timestamp, edges_out):
    """
    Process graph edges from a provider into a JSON load file for a batch time travelling load.

    This function is only suitable for the initial load in the time travelling database.
    Further loads must use a delta load algorithm.

    Edges are expected to have the following fields:
    id - the edge's unique ID.
    from - the unique ID of the vertex from where the edge originates.
    to - the unique ID of the vertex where the edge terminates.

    edgeprov - the edge provider. This is an iterable that returns edges represented as dicts.
    node_collection - the name of the collection in which the nodes associated with the edges
      reside. This is used to generate the _from and _to fields.
    load_version - the version of the load in which the edges appear.
    timestamp - the timestamp at which the edges will begin to exist.
    edges_out - a handle to the file where the edges will be written.
    
    """
    for e in edgeprov:
        e.update({
            '_key':             e['id'] + '_' + load_version,
            '_from':            node_collection + '/' + e['from'] + '_' + load_version,
            '_to':              node_collection + '/' + e['to'] + '_' + load_version,
            'first_version':    load_version,
            'last_version':     load_version,
            'created':          timestamp,
            'expires':          MAX_ADB_INTEGER,
        })
        edges_out.write(json.dumps(e) + '\n')

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

    with open(nodes) as infile, open(edges_out, 'w') as edgef:
        edgeprov = NCBIEdgeProvider(infile)
        process_edges(edgeprov, a.node_collection, a.load_version, a.load_timestamp, edgef)

if __name__  == '__main__':
    main()
