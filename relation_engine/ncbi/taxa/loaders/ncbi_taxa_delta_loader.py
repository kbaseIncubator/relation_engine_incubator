#!/usr/bin/env python

# TODO tests

import argparse
import os
import unicodedata
from arango import ArangoClient
from urllib.parse import urlparse

from relation_engine.ncbi.taxa.parsers import NCBINodeProvider
from relation_engine.ncbi.taxa.parsers import NCBIEdgeProvider
from relation_engine.ncbi.taxa.parsers import NCBIMergeProvider
from relation_engine.batchload.delta_load import load_graph_delta
from relation_engine.batchload.time_travelling_database import ArangoBatchTimeTravellingDB


NAMES_IN_FILE = 'names.dmp'
NODES_IN_FILE = 'nodes.dmp'
MERGED_IN_FILE = 'merged.dmp'

def parse_args():
    # TODO support user / pwd for db
    # TODO merge file & collection
    parser = argparse.ArgumentParser(description=
"""
Load a NCBI taxonomy dump into an AragoDB time travelling database, calculating and applying the
changes between the prior load and the current load, and retaining the prior load.
""".strip())
    parser.add_argument('--dir', required=True,
                        help='the directory containing the unzipped dump files')
    parser.add_argument(
        '--arango-url',
        required=True,
        help='The url of the arango DB server (e.g. http://localhost:8528')
    parser.add_argument(
        '--database',
        required=True,
        help='the name of the ArangoDB database that will be altered')
    parser.add_argument(
        '--node-collection',
        required=True,
        help='the name of the ArangoDB collection into which taxa nodes will be loaded')
    parser.add_argument(
        '--edge-collection',
        required=True,
        help='the name of the ArangoDB collection into which taxa edges will be loaded')
    parser.add_argument(
        '--merge-edge-collection',
        required=True,
        help='the name of the ArangoDB collection into which merge edges will be loaded')
    parser.add_argument(
        '--load-version',
        required=True,
        help='the version of this load. This version will be added to a field in the nodes and ' +
             'edges and will be used as part of the _key field.')
    parser.add_argument(
        '--load-timestamp',
        type=int,
        required=True,
        help='the timestamp to be applied to the load, in unix epoch milliseconds. Any nodes ' +
             'or edges created in this load will start to exist with this time stamp.')

    return parser.parse_args()

def main():
    a = parse_args()
    nodes = os.path.join(a.dir, NODES_IN_FILE)
    names = os.path.join(a.dir, NAMES_IN_FILE)
    merged = os.path.join(a.dir, MERGED_IN_FILE)

    url = urlparse(a.arango_url)
    client = ArangoClient(protocol=url.scheme, host=url.hostname, port=url.port)
    attdb = ArangoBatchTimeTravellingDB(
        client.db(a.database, verify=True),
        default_vertex_collection=a.node_collection,
        default_edge_collection=a.edge_collection)

    with open(nodes) as in1, open(names) as namesfile, open(nodes) as in2, open(merged) as merge:
        nodeprov = NCBINodeProvider(namesfile, in1)
        edgeprov = NCBIEdgeProvider(in2)
        merge = NCBIMergeProvider(merge)

        load_graph_delta(nodeprov, edgeprov, attdb, a.load_timestamp, a.load_version,
            merge_information=(merge, a.merge_edge_collection))

if __name__  == '__main__':
    main()