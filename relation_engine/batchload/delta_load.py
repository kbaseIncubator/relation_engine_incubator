"""
Contains a function for loading new versions of the same graph into a graph database in a batch
using a time travelling strategy
(see https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/).

Note that while the load is in progress, any queries against the graph with a timestamp after
the load timestamp are not reproducible as the load may be partially complete. Loaders must
take this into account and take measures to prevent it.
"""

from collections import defaultdict
import time

# TODO test
# TODO document reserved fields that will be overwritten if supplied
# TODO add notification callback so that the caller can implement % complete or logs or whatever based on what's happening in the delta load algorithm. Remove _VERBOSE prints at that point

# could consider threading / multiprocessing here. Virtually all the time is db access

_VERBOSE = False
_ID = 'id'
_KEY = '_key'

_BATCH_SIZE = 10000

def load_graph_delta(
        vertex_source,
        edge_source,
        database,
        timestamp,
        load_version,
        merge_source=None):
    """
    Loads a new version of a graph into a graph database, calculating the delta between the graphs
    and expiring / creating new vertices and edges as neccessary.

    vertex_source - an iterator that produces vertices as dicts. An 'id' field is required that
      uniquely identifies the vertex in this load (and any previous loads in which it exists).
    edge_source - an iterator that produces edges as dicts. An 'id' field is required that
      uniquely identifies the edge in this load (and any previous loads in which it exists).
      'from' and 'to' fields are required that identify the vertices where the edge originates and
      terminates.
    database - a wrapper for the database storing the graph. It must have the same interface as
      batchload.time_travelling_database.ArangoBatchTimeTravellingDB, which is currently the
      only implementation of the interface. The default collections will be used for the vertices
      and edges unless edge_collections is provided and the _collection field is specified for
      edges.
    timestamp - the timestamp, in Unix epoch milliseconds, when the load should be considered as
      active.
    load_version - a unique ID for this load - often the date of the data release.
    merge_source - an iterator that produces edges as dicts that represent merges of vertices.
         An 'id' field is required that uniquely identifies the edge in this load (and any previous
         loads in which it exists). 'from' and 'to' fields are required that identify the vertices
         where the edge originates (the merged vertex) and terminates (the vertex the old vertex
         was merged into). If merge_source is specified, the database must have a merge collection
         specified.
    """
    db = database
    if merge_source and not db.get_merge_collection():
        raise ValueError('A merge source is specified but the database ' +
           'has no merge collection')
    _process_verts(db, vertex_source, timestamp, load_version)
    if merge_source:
        _process_merges(db, merge_source, timestamp, load_version)
    
    if _VERBOSE: print(f'expiring vertices: {time.time()}')
    db.expire_extant_vertices_without_last_version(timestamp - 1, load_version)

    _process_edges(db, edge_source, timestamp, load_version)
    
    if _VERBOSE: print(f'expiring edges: {time.time()}')
    for col in db.get_edge_collections():
        db.expire_extant_edges_without_last_version(
            timestamp - 1, load_version, edge_collection=col)


def _process_verts(db, vertex_source, timestamp, load_version):
    count = 1
    for vertgen in _chunkiter(vertex_source, _BATCH_SIZE):
        vertices = list(vertgen)
        if _VERBOSE: print(f'vertex batch {count}: {time.time()}')
        count += 1
        keys = [v[_ID] for v in vertices]
        if _VERBOSE: print(f'  looking up {len(keys)} vertices: {time.time()}')
        dbverts = db.get_vertices(keys, timestamp)
        if _VERBOSE: print(f'  got {len(dbverts)} vertices: {time.time()}')
        bulk = db.get_batch_updater()
        for v in vertices:
            dbv = dbverts.get(v[_ID])
            if not dbv:
                bulk.create_vertex(v[_ID], load_version, timestamp, v)
            elif not _special_equal(v, dbv):
                bulk.expire_vertex(dbv[_KEY], timestamp - 1)
                bulk.create_vertex(v[_ID], load_version, timestamp, v)
            else:
                # mark node as seen in this version
                bulk.set_last_version_on_vertex(dbv[_KEY], load_version)
        if _VERBOSE: print(f'  updating {bulk.count()} vertices: {time.time()}')
        bulk.update()

def _process_merges(db, merge_source, timestamp, load_version):
    count = 1
    for mergen in _chunkiter(merge_source, _BATCH_SIZE):
        merges = list(mergen)
        if _VERBOSE: print(f'merge batch {count}: {time.time()}')
        count += 1
        keys = list({m['from'] for m in merges} | {m['to'] for m in merges})
        if _VERBOSE: print(f'  looking up {len(keys)} vertices: {time.time()}')
        dbverts = db.get_vertices(keys, timestamp)
        if _VERBOSE: print(f'  got {len(dbverts)} vertices: {time.time()}')
        bulk = db.get_batch_updater(db.get_merge_collection())
        vertbulk = db.get_batch_updater()
        for m in merges:
            dbmerged = dbverts.get(m['from'])
            dbtarget = dbverts.get(m['to'])
            # only add the merge if nodes exist at this point
            # trying to figure out where to set the edge if nodes are deleted gets complicated,
            # so we don't worry about it for now.
            if dbmerged and dbtarget:
                vertbulk.expire_vertex(dbmerged[_KEY], timestamp - 1)
                bulk.create_edge(m[_ID], dbmerged, dbtarget, load_version, timestamp)
        if _VERBOSE: print(f'  updating {bulk.count()} edges: {time.time()}')
        bulk.update()
        if _VERBOSE: print(f'  updating {vertbulk.count()} vertices: {time.time()}')
        vertbulk.update()

# assumes verts have been processed
def _process_edges(db, edge_source, timestamp, load_version):
    count = 1
    for edgegen in _chunkiter(edge_source, _BATCH_SIZE):
        edges = list(edgegen)
        if _VERBOSE: print(f'edge batch {count}: {time.time()}')
        count += 1
        keys = defaultdict(list)
        bulkset = {}
        vertkeys = set()
        for e in edges:
            # The edges exists in the current load so their nodes must exist by now
            vertkeys.add(e['to'])
            vertkeys.add(e['from'])
            col = e.pop('_collection', None)
            if not col:
                col = db.get_default_edge_collection()
            keys[col].append(e[_ID])
            if col not in bulkset:
                bulkset[col] = db.get_batch_updater(col)
        dbedges = {}
        for col, keys in keys.items():
            if _VERBOSE: print(f'  looking up {len(keys)} edges in {col}: {time.time()}')
            dbedges[col] = db.get_edges(keys, timestamp, edge_collection=col)
            if _VERBOSE: print(f'  got {len(dbedges[col])} edges: {time.time()}')
        
        # Could cache these, may be fetching the same vertex over and over, but no guarantees
        # the same vertexes are repeated in a reasonable amount of time
        # Batching the fetch is probably enough
        if _VERBOSE: print(f'  looking up {len(vertkeys)} vertices: {time.time()}')
        dbverts = db.get_vertices(list(vertkeys), timestamp)
        if _VERBOSE: print(f'  got {len(dbverts)} vertices: {time.time()}')
        keys = None
        vertkeys = None

        for e in edges:
            col = e.pop('_collection', None)
            if not col:
                col = db.get_default_edge_collection()
            dbe = dbedges[col].get(e[_ID])
            bulk = bulkset[col]
            from_ = dbverts[e['from']]
            to = dbverts[e['to']]
            if dbe:
                if (not _special_equal(e, dbe) or
                        # these two conditions check whether the nodes the edge is attached to 
                        # have been updated this load
                        dbe['_from'] != from_['_id'] or
                        dbe['_to'] != to['_id']):
                    bulk.expire_edge(dbe, timestamp - 1)
                    bulk.create_edge(e[_ID], from_, to, load_version, timestamp, e)
                else:
                    bulk.set_last_version_on_edge(dbe, load_version)
            else:
                bulk.create_edge(e[_ID], from_, to, load_version, timestamp, e)
        for b in bulkset.values():
            if _VERBOSE:
                print(f'  updating {b.count()} edges in {b.get_collection()}: {time.time()}')
            b.update()

# TODO these fields are shared between here and the database. Should probably put them somewhere in common.
# same with the id and _key fields in the code above
# arango db api is leaking a bit here, but the chance we're going to rewrite this for something
# else is pretty tiny
_SPECIAL_EQUAL_IGNORED_FIELDS = ['_id', _KEY, '_to', '_from', 'created', 'expired',
                                 'first_version', 'last_version']

def _special_equal(doc1, doc2):
    """
    Checks if two dicts are equal other than special fields.
    """
    d1c = dict(doc1)
    d2c = dict(doc2)

    for f in _SPECIAL_EQUAL_IGNORED_FIELDS:
        d1c.pop(f, None)
        d2c.pop(f, None)
    
    return d1c == d2c 

def _chunkiter(iterable, size):
  def inneriter(first, iterator, size):
    yield first
    for _ in range(size - 1): 
      yield next(iterator)
  it = iter(iterable)
  while True:
    yield inneriter(next(it), it, size)