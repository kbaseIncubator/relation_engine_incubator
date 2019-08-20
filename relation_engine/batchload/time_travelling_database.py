"""
Classes for interfacing with the KBase relation engine database, specifically with graphs that
are versioned with batch time travelling (see 
https://github.com/kbaseIncubator/relation_engine_incubator/blob/master/delta_load_algorithm.md)

The classes and methods here were created for the purpose of supporting the graph delta loader, but
more classes and methods can be added as needed.

"""

from arango.exceptions import CursorEmptyError as _CursorEmptyError

_INTERNAL_ARANGO_FIELDS = ['_rev']

_FLD_KEY = '_key'
_FLD_FULL_ID = '_id'
_FLD_ID = 'id'

_FLD_FROM = '_from'
_FLD_FROM_ID = 'from'
_FLD_TO = '_to'
_FLD_TO_ID = 'to'

_FLD_VER_LST = 'last_version'
_FLD_VER_FST = 'first_version'
_FLD_CREATED = 'created'
_FLD_EXPIRED = 'expired'

# see https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/
# in unix epoch ms this is 2255/6/5
_MAX_ADB_INTEGER = 2**53 - 1

class ArangoBatchTimeTravellingDB:
    """
    A collection of methods for inserting and retrieving data from ArangoDB.
    """

    # may want to make this an NCBIRelationEngine interface, but pretty unlikely we'll switch...

    def __init__(
            self,
            database,
            vertex_collection,
            default_edge_collection=None,
            edge_collections=None):
        """
        Create the DB interface.

        database - the python_arango ArangoDB database containing the data to query or modify.
        vertex_collection - the name of the collection to use for vertex operations.
        default_edge_collection - the name of the collection to use for edge operations by default.
          This can be overridden.
        edge_collections - a list of any edge collections in the graph.
          The collections are checked for existence and cached for performance reasons.

        Specifying an edge collection in a method argument that is not in edge_collections or
        is not the defalt collection will result in an error.
        """
        self._default_edge_collection = default_edge_collection
        edgecols = set()
        if default_edge_collection:
            edgecols.add(default_edge_collection)
        if edge_collections:
            edgecols.update(edge_collections)
        if not edgecols:
            raise ValueError("At least one edge collection must be specified")
        self._database = database
        self._vertex_collection = self._get_col(vertex_collection)

        self._edgecols = {n: self._get_col(n, edge=True) for n in edgecols}

    # if an edge is inserted into a non-edge collection _from and _to are silently dropped
    def _get_col(self, collection, edge=False):
        c = self._database.collection(collection)
        if not c.properties()['edge'] is edge: # this is a http call
            ctype = 'an edge' if edge else 'a vertex'
            raise ValueError(f'{collection} is not {ctype} collection')
        return c

    def get_vertex_collection(self):
        """
        Returns the name of the vertex collection.
        """
        return self._vertex_collection.name

    def get_default_edge_collection(self):
      """
      Returns the name of the default edge collection or None.
      """
      return self._default_edge_collection

    def get_edge_collections(self):
        """
        Returns the names of all the registered edge collections as a list, including the default
        collection, if any.
        """
        return sorted(list(self._edgecols.keys()))

    def get_vertices(self, ids, timestamp):
        """
        Get vertices that exist at the given timestamp from a collection.

        A vertex ID and a timestamp uniquely identifies a vertex in a collection.

        ids - the IDs of the vertices to get.
        timestamp - the time at which the vertices must exist in Unix epoch milliseconds.

        Returns a dict of vertex ID -> vertex. Missing vertices are not included and do not
          cause an error.
        """
        col_name = self._vertex_collection.name
        return self._get_documents(ids, timestamp, col_name)

    def _get_documents(self, ids, timestamp, collection_name):
        cur = self._database.aql.execute(
          f"""
          FOR d IN @@col
              FILTER d.{_FLD_ID} IN @ids
              FILTER d.{_FLD_EXPIRED} >= @timestamp AND d.{_FLD_CREATED} <= @timestamp
              RETURN d
          """,
          bind_vars={'ids': ids, 'timestamp': timestamp, '@col': collection_name},
          count=True
        )
        ret = {}
        try:
            for d in cur:
                if d[_FLD_ID] in ret:
                    raise ValueError(f'db contains > 1 document for id {d[_FLD_ID]}, ' +
                        f'timestamp {timestamp}, collection {collection_name}')
                ret[d[_FLD_ID]] = self._clean(d)
        finally:
            cur.close(ignore_missing=True)
        return ret

    def get_edges(self, ids, timestamp, edge_collection=None):
        """
        Get edges that exist at the given timestamp from a collection.

        An edge ID and a timestamp uniquely identifies an edge in a collection.

        ids - the IDs of the edges to get.
        timestamp - the time at which the edges must exist in Unix epoch milliseconds.
        edge_collection - the collection name to query. If none is provided, the default will
          be used.

        Returns a dict of edge ID -> edge. Missing edges are not included and do not
          cause an error.
        """
        col_name = self._get_edge_collection(edge_collection).name
        return self._get_documents(ids, timestamp, col_name)

    def save_vertex(self, id_, version, created_time, data):
        """
        Save a vertex in the database.

        Note that only a shallow copy of the data is made before adding database fields. Modifying
        embedded data structures may have unexpected results.

        The _key field is generated from the id_ and version fields, which are expected to uniquely
        identify a vertex.

        id_ - the external ID of the vertex.
        version - the version of the load as part of which the vertex is being created.
        created_time - the time at which the vertex should begin to exist in Unix epoch
          milliseconds.
        data - the vertex contents as a dict.

        Returns the key for the vertex.
        """

        data = _create_vertex(data, id_, version, created_time)
        self._vertex_collection.insert(data, silent=True)
        return data[_FLD_KEY]

    def save_edge(
            self,
            id_,
            from_vertex,
            to_vertex,
            version,
            created_time,
            data=None,
            edge_collection=None):
        """
        Save an edge in the database.

        Note that only a shallow copy of the data is made before adding database fields. Modifying
        embedded data structures may have unexpected results.

        The _key field is generated from the id_ and version fields, which are expected to uniquely
        identify an edge.

        id_ - the external ID of the edge.
        from_vertex - the vertex where the edge originates. This vertex must have been fetched from
          the database.
        to_vertex - the vertex where the edge terminates. This vertex must have been fetched from
          the database.
        version - the version of the load as part of which the edge is being created.
        created_time - the time at which the edge should begin to exist in Unix epoch milliseconds.
        data - the edge contents as a dict.
        edge_collection - the name of the collection to modify. If none is provided, the default
          will be used.

        Returns the key for the edge.
        """
        data = _create_edge(id_, from_vertex, to_vertex, version, created_time, data)
        col = self._get_edge_collection(edge_collection)
        col.insert(data, silent=True)
        return data[_FLD_KEY]

    def set_last_version_on_vertex(self, key, last_version):
        """
        Set the last version field on a vertex.

        key - the key of the vertex.
        last_version - the version to set.
        """
        self._vertex_collection.update({_FLD_KEY: key, _FLD_VER_LST: last_version}, silent=True)

    def set_last_version_on_edge(self, key, last_version, edge_collection=None):
        """
        Set the last version field on a edge.

        key - the key of the edge.
        last_version - the version to set.
        edge_collection - the collection name to query. If none is provided, the default will
          be used.
        """
        col = self._get_edge_collection(edge_collection)

        col.update({_FLD_KEY: key, _FLD_VER_LST: last_version}, silent=True)

    def expire_vertex(self, key, expiration_time):
        """
        Sets the expiration time on a vertex and adjacent edges in the given collections.

        key - the vertex key.
        expiration_time - the time, in Unix epoch milliseconds, to set as the expiration time
          on the vertex and any affected edges.
        """
        self._vertex_collection.update({_FLD_KEY: key, _FLD_EXPIRED: expiration_time}, silent=True)

    def expire_edge(self, key, expiration_time, edge_collection=None):
        """
        Sets the expiration time on an edge in the given collections.

        key - the edge key.
        expiration_time - the time, in Unix epoch milliseconds, to set as the expiration time
          on the edge.
        edge_collection - the collection name to query. If none is provided, the default will
          be used.

        """
        col = self._get_edge_collection(edge_collection)
        col.update({_FLD_KEY: key, _FLD_EXPIRED: expiration_time}, silent=True)

    # may need to separate timestamp into find and expire timestamps, but YAGNI for now
    def expire_extant_vertices_without_last_version(self, timestamp, version):
        """
        Expire all vertices that exist at the given timestamp where the last version field is 
        not equal to the given version. The expiration date will be the given timestamp.

        timestamp - the timestamp to use to find extant vertices as well as the timestamp to use
          as the expiration date.
        version - the version required for the last version field for a vertex to avoid expiration.
        """
        col_name = self._vertex_collection.name
        self._expire_extant_document_without_last_version(timestamp, version, col_name)

    # may need to separate timestamp into find and expire timestamps, but YAGNI for now
    def expire_extant_edges_without_last_version(
            self,
            timestamp,
            version,
            edge_collection=None):
        """
        Expire all edges that exist at the given timestamp where the last version field is 
        not equal to the given version. The expiration date will be the given timestamp.

        timestamp - the timestamp to use to find extant edges as well as the timestamp to use
          as the expiration date.
        version - the version required for the last version field for a edges to avoid expiration.
        edge_collection - the collection name to query. If none is provided, the default will
          be used.
        """
        col_name = self._get_edge_collection(edge_collection).name
        self._expire_extant_document_without_last_version(timestamp, version, col_name)
    
    def _expire_extant_document_without_last_version(self, timestamp, version, col_name):
        self._database.aql.execute(
          f"""
          FOR d IN @@col
              FILTER d.{_FLD_EXPIRED} >= @timestamp && d.{_FLD_CREATED} <= @timestamp
              FILTER d.{_FLD_VER_LST} != @version
              UPDATE d WITH {{{_FLD_EXPIRED}: @timestamp}} IN @@col
          """,
          bind_vars={'version': version, 'timestamp': timestamp, '@col': col_name},
        )

    # mutates in place!
    def _clean(self, obj):
        for k in _INTERNAL_ARANGO_FIELDS:
            del obj[k] 
        return obj

    def _get_edge_collection(self, collection):
        if not collection:
            if not self._default_edge_collection:
                raise ValueError('No default edge collection specified, ' +
                    'must specify edge collection')
            return self._edgecols[self._default_edge_collection]
        if collection not in self._edgecols:
            raise ValueError(f'Edge collection {collection} was not registered at initialization')
        return self._edgecols[collection]

    def get_batch_updater(self, edge_collection_name=None):
        """
        Get a batch updater for a collection. Updates can be added to the updater and then
        applied at once.

        edge_collection_name - the name of the edge collection that will be updated. If not
          provided the vertex collection is used.

        Returns a BatchUpdater.
        """
        if not edge_collection_name:
            return BatchUpdater(self._vertex_collection, False)
        return BatchUpdater(self._get_edge_collection(edge_collection_name), True)

class BatchUpdater:

    def __init__(self, collection, edge=False):
        """
        Do not create this class directly - call ArangoBatchTimeTravellingDB.get_batch_updater().

        This class is not thread safe.

        Create a batch updater.

        collection - the python-arango collection where updates will be applied.
        edge - True if the collection is an edge collection. Checking this property requires
          an http call, and so providing the type is required.

        Properties:
        is_edge - True if the updater will update against an edge collection, false otherwise.
        """
        self._col = collection
        self.is_edge = edge
        self._updates = []

    def get_collection(self):
        """
        Return the name of the collection to which updates will be applied.
        """
        return self._col.name

    def create_vertex(self, id_, version, created_time, data):
        """
        Save a vertex in the database.

        Note that only a shallow copy of the data is made before adding database fields. Modifying
        embedded data structures may have unexpected results.

        The _key field is generated from the id_ and version fields, which are expected to uniquely
        identify a vertex.

        id_ - the external ID of the vertex.
        version - the version of the load as part of which the vertex is being created.
        created_time - the time at which the vertex should begin to exist in Unix epoch
          milliseconds.
        data - the vertex contents as a dict.

        Returns the key for the vertex.
        """
        if self.is_edge:
            raise ValueError('Batch updater is configured for an edge collection')
        vert = _create_vertex(data, id_, version, created_time)
        self._updates.append(vert)
        return vert[_FLD_KEY]

    def create_edge(self, id_, from_vertex, to_vertex, version, created_time, data=None):
        """
        Save an edge in the database.

        Note that only a shallow copy of the data is made before adding database fields. Modifying
        embedded data structures may have unexpected results.

        The _key field is generated from the id_ and version fields, which are expected to uniquely
        identify an edge.

        id_ - the external ID of the edge.
        from_vertex - the vertex where the edge originates. This vertex must have been fetched from
          the database.
        to_vertex - the vertex where the edge terminates. This vertex must have been fetched from
          the database.
        version - the version of the load as part of which the edge is being created.
        created_time - the time at which the edge should begin to exist in Unix epoch milliseconds.
        data - the edge contents as a dict.

        Returns the key for the edge.
        """
        if not self.is_edge:
            raise ValueError('Batch updater is configured for a vertex collection')
        edge = _create_edge(id_, from_vertex, to_vertex, version, created_time, data)
        self._updates.append(edge)
        return edge[_FLD_KEY]

    def update(self):
        """
        Apply the updates collected so far and clear the update list.
        """
        self._col.import_bulk(self._updates, on_duplicate="update")
        self._updates.clear()

def _create_vertex(data, id_, version, created_time):
    data = dict(data) # make a copy and overwrite the old data variable
    data[_FLD_KEY] = id_ + '_' + version
    data[_FLD_ID] = id_
    data[_FLD_VER_FST] = version
    data[_FLD_VER_LST] = version
    data[_FLD_CREATED] = created_time
    data[_FLD_EXPIRED] = _MAX_ADB_INTEGER

    return data

def _create_edge(
        id_,
        from_vertex,
        to_vertex,
        version,
        created_time,
        data):
    data = {} if not data else data

    data = dict(data) # make a copy and overwrite the old data variable
    data[_FLD_KEY] = id_ + '_' + version
    data[_FLD_ID] = id_
    data[_FLD_FROM] = from_vertex[_FLD_FULL_ID]
    data[_FLD_FROM_ID] = from_vertex[_FLD_ID]
    data[_FLD_TO] = to_vertex[_FLD_FULL_ID]
    data[_FLD_TO_ID] = to_vertex[_FLD_ID]
    data[_FLD_VER_FST] = version
    data[_FLD_VER_LST] = version
    data[_FLD_CREATED] = created_time
    data[_FLD_EXPIRED] = _MAX_ADB_INTEGER
    return data