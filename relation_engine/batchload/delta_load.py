"""
Contains a function for loading new versions of the same graph into a graph database in a batch
using a time travelling strategy
(see https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/).

Note that while the load is in progress, any queries against the graph with a timestamp after
the load timestamp are not reproducible as the load may be partially complete. Loaders must
take this into account and take measures to prevent it.
"""

# TODO test
# TODO document reserved fields that will be overwritten if supplied

_ID = 'id'
_KEY = '_key'

def load_graph_delta(
        vertex_source,
        edge_source,
        database,
        timestamp,
        load_version,
        edge_collections=None,
        merge_information=None):
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
    edge_collections - the list of collections that may contain edges (other than merge edges).
      This list must include any collection names specified by the _collection field in edges,
      and is used to properly expire edges connected to expired vertices. If the only collection
      used is the default collection from database.get_default_edge_collection() this argument
      is not required.
    merge_information - a tuple with two entries:
      1) an iterator that produces edges as dicts that represent merges of vertices.
         An 'id' field is required that uniquely identifies the edge in this load (and any previous
         loads in which it exists). 'from' and 'to' fields are required that identify the vertices
         where the edge originates (the merged vertex) and terminates (the vertex the old vertex
         was merged into.).
      2) The name of the collection where merge edges should be stored.
    """
    db = database
    # TODO spec edge collections in DB constructor & cache objects
    # TODO check vertex default collection is not none, check edge_collections are edge collections
    edge_cols = []
    if db.get_default_edge_collection():
        edge_cols.append(db.get_default_edge_collection())
    if merge_information:
        edge_cols.append(merge_information[1])
    if edge_collections:
        edge_cols.extend(edge_collections)
    
    # count = 0
    for v in vertex_source:
        # could batch things up here if slow
        dbv = db.get_vertex(v[_ID], timestamp)
        if not dbv:
            db.save_vertex(v[_ID], load_version, timestamp, v)
        elif not _special_equal(v, dbv):
            db.expire_vertex(dbv[_KEY], timestamp - 1, edge_collections=edge_cols)
            db.save_vertex(v[_ID], load_version, timestamp, v)
        else:
            # mark node as seen in this version
            db.set_last_version_on_vertex(dbv[_KEY], load_version)
        # count += 1
        # if count % 1000 == 0:
        #     print(f'node {count}')

    if merge_information:
        for m in merge_information[0]:
            dbmerged = db.get_vertex(m['from'], timestamp)
            dbtarget = db.get_vertex(m['to'], timestamp)
            # only add the merge if nodes exist at this point
            # trying to figure out where to set the edge if nodes are deleted gets complicated,
            # so we don't worry about it for now.
            if dbmerged and dbtarget:
                db.expire_vertex(dbmerged[_KEY], timestamp - 1, edge_collections=edge_cols)
                db.save_edge(m[_ID], dbmerged, dbtarget, load_version, timestamp,
                    edge_collection=merge_information[1])

    # print('del nodes')
    db.expire_extant_vertices_without_last_version(timestamp - 1, load_version)

    # count = 0
    for e in edge_source:
        # could batch things up here if slow
        col = db.get_default_edge_collection()
        if e.get('_collection'):
            col = e.pop('_collection')
            if col not in edge_collections:
                raise ValueError(f'Collection {col} not in provided collection list')
        dbe = db.get_edge(e[_ID], timestamp, edge_collection=col)
        # The edge exists in the current load so its nodes must exist by now
        # Could cache these, may be fetching the same vertex over and over, but no guarantees
        # the same vertexes are repeated in a reasonable amount of time
        from_ = db.get_vertex(e['from'], timestamp)
        to = db.get_vertex(e['to'], timestamp)
        if dbe:
            if (not _special_equal(e, dbe) or
                    # these two conditions check whether the nodes the edge is attached to 
                    # have been updated this load
                    dbe['_from'] != from_['_id'] or
                    dbe['_to'] != to['_id']):
                db.expire_edge(dbe[_KEY], timestamp - 1, edge_collection=col)
                db.save_edge(e[_ID], from_, to, load_version, timestamp, e, edge_collection=col)
            else:
                db.set_last_version_on_edge(dbe[_KEY], load_version, edge_collection=col)
        else:
            db.save_edge(e[_ID], from_, to, load_version, timestamp, e, edge_collection=col)
        # count += 1
        # if count % 1000 == 0:
        #     print(f'edge {count}')

    # print('del edges')
    if merge_information:
        edge_cols.remove(merge_information[1])
    for col in edge_cols:
        db.expire_extant_edges_without_last_version(
            timestamp - 1, load_version, edge_collection=col)

# TODO these fields are shared between here and the database. Should probably put them somewhere in common.
# same iwth the id and _key fields in the code above
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