#!/usr/bin/env python

# TODO TEST

import argparse
import getpass
import os
import unicodedata
from arango import ArangoClient
from urllib.parse import urlparse

from relation_engine.ncbi.taxa.parsers import NCBINodeProvider
from relation_engine.ncbi.taxa.parsers import NCBIEdgeProvider
from relation_engine.ncbi.taxa.parsers import NCBIMergeProvider
from relation_engine.batchload.delta_load import load_graph_delta
from relation_engine.batchload.time_travelling_database import ArangoBatchTimeTravellingDB

_LOAD_NAMESPACE = 'ncbi_taxa'

NAMES_IN_FILE = 'names.dmp'
NODES_IN_FILE = 'nodes.dmp'
MERGED_IN_FILE = 'merged.dmp'

def parse_args():
    parser = argparse.ArgumentParser(description=
"""
Load a NCBI taxonomy dump into an ArangoDB time travelling database, calculating and applying the
changes between the prior load and the current load, and retaining the prior load.
""".strip())
    parser.add_argument('--dir', required=True,
                        help='the directory containing the unzipped dump files')
    parser.add_argument(
        '--arango-url',
        required=True,
        help='The url of the ArangoDB server (e.g. http://localhost:8528')
    parser.add_argument(
        '--database',
        required=True,
        help='the name of the ArangoDB database that will be altered')
    parser.add_argument(
        '--user',
        help='the ArangoDB user name; if --pwd-file is not included a password prompt will be ' +
            'presented. Omit to connect with default credentials.')
    parser.add_argument(
        '--pwd-file',
        help='the path to a file containing the ArangoDB password and nothing else; ' +
            'if --user is included and --pwd-file is omitted a password prompt will be presented.')
    parser.add_argument(
        '--load-registry-collection',
        required=True,
        help='the name of the ArangoDB collection where the load will be registered. ' +
            'This is typically the same collection for all delta loaded data.')
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
             'or edges created in this load will start to exist with this time stamp. ' +
             'NOTE: the user is responsible for ensuring this timestamp is greater than any ' +
             'other timestamps previously used to load data into the NCBI taxonomy DB.')
    parser.add_argument(
        '--release-timestamp',
        type=int,
        required=True,
        help='the timestamp, in unix epoch milliseconds, when the data was released ' +
            'at the source.')

    return parser.parse_args()

def main():
    a = parse_args()
    nodes = os.path.join(a.dir, NODES_IN_FILE)
    names = os.path.join(a.dir, NAMES_IN_FILE)
    merged = os.path.join(a.dir, MERGED_IN_FILE)

    url = urlparse(a.arango_url)
    client = ArangoClient(protocol=url.scheme, host=url.hostname, port=url.port)
    if a.user:
        if a.pwd_file:
            with open(a.pwd_file) as pwd_file:
                pwd = pwd_file.read().strip()
        else:
            pwd = getpass.getpass()
        db = client.db(a.database, a.user, pwd, verify=True)
    else:
        db = client.db(a.database, verify=True)
    attdb = ArangoBatchTimeTravellingDB(
        db,
        a.load_registry_collection,
        a.node_collection,
        default_edge_collection=a.edge_collection,
        merge_collection=a.merge_edge_collection)

    with open(nodes) as in1, open(names) as namesfile, open(nodes) as in2, open(merged) as merge:
        nodeprov = NCBINodeProvider(namesfile, in1)
        edgeprov = NCBIEdgeProvider(in2)
        merge = NCBIMergeProvider(merge)

        load_graph_delta(_LOAD_NAMESPACE, nodeprov, edgeprov, attdb,
            a.load_timestamp, a.release_timestamp, a.load_version, merge_source=merge)

if __name__  == '__main__':
    main()