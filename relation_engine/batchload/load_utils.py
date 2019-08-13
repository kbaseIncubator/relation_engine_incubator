""" 
Utilities for loading graph data into the relation engine.
"""

# TODO tests

import json
import unicodedata

# assumes there's at least one non-whitespace char in string
def canonicalize(string, ignore_tokens):
    """
    Canonicalizes a string by:
    Lowercasing
    Unicode normalization
    Tokenizing
    Removing non-alphanumeric characters from each end of each token
    Ignoring any tokens in the ignore_tokens set
    """
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

# TODO this should probably be a parameter? YAGNI for now
# see https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/
# in unix epoch ms this is 2255/6/5
_MAX_ADB_INTEGER = 2**53 - 1

def process_nodes(nodeprov, load_version, timestamp, nodes_out):
    """
    Process graph nodes from a provider into a JSON load file for a batch time travelling load.

    This function is only suitable for the initial load in the time travelling database.
    Further loads must use a delta load algorithm.

    Nodes are expected to have an 'id' field containing the node's unique ID.

    nodeprov - the node provider. This is an iterable that returns nodes represented as dicts.
    load_version - the version of the load in which the nodes appear. This is expected to be
      unique per load.
    timestamp - the timestamp at which the nodes will begin to exist.
    nodes_out - a handle to the file where the nodes will be written.
    """
    for n in nodeprov:
        n.update({
            '_key':           n['id'] + '_' + load_version,
            'first_version':  load_version,
            'last_version':   load_version,
            'created':        timestamp,
            'expired':        _MAX_ADB_INTEGER
            })
        nodes_out.write(json.dumps(n) + '\n')

def process_edge(edge, node_collection, load_version, timestamp):
    """
    Note that this funtion modifies the edge argument in place.

    Process a graph edge for a batch time travelling load.
    Adds appropriate fields to the edge. 

    This function is only suitable for the initial load in the time travelling database.
    Further loads must use a delta load algorithm.

    Edges are expected to have the following fields:
    id - the edge's unique ID.
    from - the unique ID of the vertex from where the edge originates.
    to - the unique ID of the vertex where the edge terminates.

    edge - the edge as a dict.
    node_collection - the name of the collection in which the nodes associated with the edge
      reside. This is used to generate the _from and _to fields.
    load_version - the version of the load in which the edge appears. This is expected to be
      unique per load.
    timestamp - the timestamp at which the edge will begin to exist.

    Returns - the updated edge as a dict.
    """
    edge.update({
            '_key':             edge['id'] + '_' + load_version,
            '_from':            node_collection + '/' + edge['from'] + '_' + load_version,
            '_to':              node_collection + '/' + edge['to'] + '_' + load_version,
            'first_version':    load_version,
            'last_version':     load_version,
            'created':          timestamp,
            'expired':          _MAX_ADB_INTEGER,
        })
    return edge

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
    load_version - the version of the load in which the edges appear. This is expected to be
      unique per load.
    timestamp - the timestamp at which the edges will begin to exist.
    edges_out - a handle to the file where the edges will be written.
    """
    for e in edgeprov:
        e = process_edge(e, node_collection, load_version, timestamp)
        edges_out.write(json.dumps(e) + '\n')