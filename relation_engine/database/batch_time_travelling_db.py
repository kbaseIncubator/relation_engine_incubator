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
        # for some reason I don't understand these collections count as False
        self._has_vert_col = False
        self._has_edge_col = False
        if default_vertex_collection:
            self._default_vertex_collection = self._database.collection(default_vertex_collection)
            self._has_vert_col = True
        else:
            self._default_vertex_collection = None
    
        if default_edge_collection:
            self._default_edge_collection = self._ensure_edge_col(default_edge_collection)
            self._has_edge_col = True
        else:
            self._default_edge_collection = None

    # if an edge is inserted into a non-edge collection _from and _to are silently dropped
    def _ensure_edge_col(self, collection):
        c = self._database.collection(collection)
        if not c.properties()['edge']:
            raise ValueError(f'{collection} is not an edge collection')
        return c

    def get_vertex(self, id_, timestamp, vertex_collection=None):
        """
        Get a vertex from a collection that exists at the given timestamp.

        A vertex ID and a timestamp uniquely identifies a vertex in a collection.

        id_ - the ID of the vertex.
        timestamp - the time at which the vertex must exist in Unix epoch milliseconds.
        vertex_collection - the collection name to query. If none is provided, the default will
          be used.
        """
        col_name = self._get_vertex_collection(vertex_collection).name
        return self._get_document(id_, timestamp, col_name)

    def _get_document(self, id_, timestamp, collection_name):
        cur = self._database.aql.execute(
          f"""
          FOR d IN @@col
              FILTER d.{_FLD_ID} == @id
              FILTER d.{_FLD_CREATED} <= @timestamp && d.{_FLD_EXPIRED} >= @timestamp
              RETURN d
          """,
          bind_vars={'id': id_, 'timestamp': timestamp, '@col': collection_name},
          count=True
        )
        if cur.count() > 1:
            raise ValueError(f'db contains > 1 document for id {id_}, timestamp {timestamp}, ' +
                             f'collection {collection_name}')
        
        try:
            d = self._clean(cur.pop())
        except _CursorEmptyError as _:
            d = None
        cur.close()
        return d

    def get_edge(self, id_, timestamp, edge_collection=None):
        """
        Get an edge from a collection that exists at the given timestamp.

        An edge ID and a timestamp uniquely identifies an edge in a collection.

        id_ - the ID of the edge.
        timestamp - the time at which the vertex must exist in Unix epoch milliseconds.
        edge_collection - the collection name to query. If none is provided, the default will
          be used.
        """
        col_name = self._get_edge_collection(edge_collection).name
        return self._get_document(id_, timestamp, col_name)


    def save_vertex(self, id_, version, created_time, data, vertex_collection=None):
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
        vertex_collection - the name of the collection to modify. If none is provided, the default
          will be used.

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
        col = self._get_edge_collection(edge_collection)
        data = {} if not data else data

        # May want a bulk method
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

    def expire_vertex(self, key, expiration_time, edge_collections=None, vertex_collection=None):
        """
        Sets the expiration time on a vertex and adjacent edges in the given collections.

        key - the vertex key.
        expiration_time - the time, in Unix epoch milliseconds, to set as the expiration time
          on the vertex and any affected edges.
        edge_collections - a list of names of collections that will be checked for connected
          edges.
        vertex_collection - the collection name to query. If none is provided, the default will
          be used.

        """
        edge_collections = [] if not edge_collections else edge_collections
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

    # mutates in place!
    def _clean(self, obj):
        for k in _INTERNAL_ARANGO_FIELDS:
            del obj[k] 
        return obj

    def _get_vertex_collection(self, collection):
        # TODO handle no such collection
        if collection:
            return self._database.collection(collection)
        if not self._has_vert_col:
            raise ValueError("No collection provided and no default specified")
        return self._default_vertex_collection

    def _get_edge_collection(self, collection):
        # TODO handle no such collection
        if collection:
            return self._ensure_edge_col(collection)
        if not self._has_edge_col:
            raise ValueError("No collection provided and no default specified")
        return self._default_edge_collection
