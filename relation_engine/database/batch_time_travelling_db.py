"""
Classes for interfacing with the KBase relation engine database, specifically with graphs that
are versioned with batch time travelling (see 
https://github.com/kbaseIncubator/relation_engine_incubator/blob/master/delta_load_algorithm.md)

The classes and methods here were created for the purpose of supporting the graph delta loader, but
more classes and methods can be added as needed.

"""

from arango.exceptions import CursorEmptyError

_INTERNAL_ARANGO_FIELDS = ['_id', '_rev']

_FLD_KEY = '_key'
_FLD_ID = 'id'
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

    def __init__(self, database, default_vertex_collection=None, default_edge_collection=None):
        """
        Create the DB interface.

        database - the python_arango ArangoDB database containing the data to query or modify.
        default_vertex_collection - the name of the collection to use for vertex operations.
          This can be overridden.
        default_edge_collection - the name of the collection to use for edge operations.
          This can be overridden.
        """
        self._database = database
        self._default_vertex_collection = default_vertex_collection
        self._default_edge_collection = default_edge_collection

    def get_vertex(self, id_, timestamp, vertex_collection=None):
        """
        Get a vertex from a collection that exists at the given timestamp.

        A node ID and a timestamp uniquely identifies a node in a collection.

        id_ - the ID of the vertex.
        timestamp - the time at which the node must exist in Unix epoch milliseconds.
        vertex_collection - the collection name to query. If none is provided, the default will
          be used.
        """
        col_name = self._get_vertex_collection(vertex_collection).name
        cur = self._database.aql.execute(
          f"""
          FOR v IN @@col
              FILTER v.{_FLD_ID} == @id
              FILTER v.{_FLD_CREATED} <= @timestamp && v.{_FLD_EXPIRED} >= @timestamp
              RETURN v
          """,
          bind_vars={'id': id_, 'timestamp': timestamp, '@col': col_name},
          count=True
        )
        if cur.count() > 1:
            raise ValueError(f'db contains > 1 vertex for id {id_}, timestamp {timestamp}, ' +
                             f'collection {col_name}')
        
        try:
            v = self._clean(cur.pop())
        except CursorEmptyError as _:
            v = None
        cur.close()
        return v

    def save_vertex(self, id_, version, created_time, data, vertex_collection=None):
        """
        Save a vertex in the database.

        Note that only a shallow copy of the data is made before adding database fields. Modifying
        embedded data structures may have unexpected results.

        The _key field is generated from the id_ and version fields, which are expected to uniquely
        identify a node.

        id_ - the external ID of the node.
        version - the version of the load as part of which the vertex is being created.
        created_time - the time at which the node should begin to exist in Unix epoch milliseconds.
        data - the node contents as a dict.
        vertex_collection - the collection name to query. If none is provided, the default will
          be used.

        Returns the key for the vertex.
        """

        col = self._get_vertex_collection(vertex_collection)

        # May want a bulk method
        data = dict(data) # make a copy and overwrite the old data variable
        data[_FLD_KEY] = id_ + '_' + version
        data[_FLD_ID] = id_
        data[_FLD_VER_FST] = version
        data[_FLD_VER_LST] = version
        data[_FLD_CREATED] = created_time
        data[_FLD_EXPIRED] = _MAX_ADB_INTEGER

        col.insert(data, silent=True)
        return data[_FLD_KEY]

    def set_last_version_on_vertex(self, key, last_version, vertex_collection=None):
        """
        Set the last version field on a vertex.

        key - the key of the vertex.
        last_version - the version to set.
        vertex_collection - the collection name to query. If none is provided, the default will
          be used.
        """
        col = self._get_vertex_collection(vertex_collection)

        col.update({_FLD_KEY: key, _FLD_VER_LST: last_version}, silent=True)

    def expire_vertex(self, key, expiration_time, edge_collections=None, vertex_collection=None):
        """
        Sets the expiration time on a vertex and adjacent edges in the given collections.

        key - the node key.
        expiration_time - the time, in Unix epoch milliseconds, to set as the expiration time
          on the node and any affected edges.
        edge_collections - a list of names of collections that will be checked for connected
          edges.
        vertex_collection - the collection name to query. If none is provided, the default will
          be used.

        """
        edge_collections = [] if edge_collections is None else edge_collections
        col = self._get_vertex_collection(vertex_collection)
        # filter out nulls or empty strings and fail early on missing collections
        edge_names = [self._get_edge_collection(e).name for e in edge_collections if e]

        # you can only do updates on one collection at once
        for ec in edge_names:
            self._database.aql.execute(
            f"""
            WITH @@vcol FOR v, e IN 1 ANY @start @@ecol
                UPDATE e WITH {{{_FLD_EXPIRED}: @timestamp}} IN @@ecol

            """,
            bind_vars={'@vcol': col.name,
                       '@ecol': ec,
                       'start': col.name + '/' + key,
                       'timestamp': expiration_time
                       },
            )
        col.update({_FLD_KEY: key, _FLD_EXPIRED: expiration_time}, silent=True)

    # mutates in place!
    def _clean(self, obj):
        for k in _INTERNAL_ARANGO_FIELDS:
            del obj[k] 
        return obj

    def _get_vertex_collection(self, collection):
        # TODO handle no such collection
        if collection:
            return self._database.collection(collection)
        if not self._default_vertex_collection:
            raise ValueError("No collection provided and no default specified")
        return self._database.collection(self._default_vertex_collection)

    def _get_edge_collection(self, collection):
        # TODO handle no such collection
        if collection:
            return self._database.collection(collection)
        if not self._default_edge_collection:
            raise ValueError("No collection provided and no default specified")
        return self._database.collection(self._default_edge_collection)
