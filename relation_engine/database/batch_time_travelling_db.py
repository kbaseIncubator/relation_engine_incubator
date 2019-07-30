"""
Classes for interfacing with the KBase relation engine database, specifically with graphs that
are versioned with batch time travelling (see 
https://github.com/kbaseIncubator/relation_engine_incubator/blob/master/delta_load_algorithm.md)

The classes and methods here were created for the purpose of supporting the graph delta loader, but
more classes and methods can be added as needed.

"""

from arango.exceptions import CursorEmptyError

_INTERNAL_ARANGO_FIELDS = ['_id', '_rev']

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
        col = self._get_vertex_collection(vertex_collection)
        cur = self._database.aql.execute(
          f"""
          FOR v IN {col.name}
              FILTER v.id == @id
              FILTER v.created <= @timestamp && v.expires >= @timestamp
              RETURN v
          """,
          bind_vars={'id': id_, 'timestamp': timestamp},
          count=True
        )
        if cur.count() > 1:
            raise ValueError(f'db contains > 1 vertex for id {id_}, timestamp {timestamp}, ' +
                             'collection {col.name}')
        
        try:
            v = self._clean(cur.pop())
        except CursorEmptyError as _:
            v = None
        cur.close()
        return v

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
