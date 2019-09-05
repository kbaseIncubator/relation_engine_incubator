#!/usr/bin/env python

# TODO TEST

import argparse
import getpass
import os
from arango import ArangoClient
from urllib.parse import urlparse

from relation_engine.batchload.delta_load import roll_back_last_load
from relation_engine.batchload.time_travelling_database import ArangoBatchTimeTravellingDBFactory


def parse_args():
    parser = argparse.ArgumentParser(description=
"""
Roll back a delta load in a data namespace.

The most recent load will be removed.
""".strip())
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
        '--load-namespace',
        required=True,
        help='the name of the data that is being rollec back, e.g. envo, gene_ontology, etc. ' +
            'Must be unique across all load sources and consistent across loads.')
    parser.add_argument(
        '--load-registry-collection',
        required=True,
        help='the name of the ArangoDB collection where loads are registered. ' +
            'This is typically the same collection for all delta loaded data.')

    return parser.parse_args()

def main():
    a = parse_args()

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
    fac = ArangoBatchTimeTravellingDBFactory(db, a.load_registry_collection)

    roll_back_last_load(fac, a.load_namespace)

if __name__  == '__main__':
    main()