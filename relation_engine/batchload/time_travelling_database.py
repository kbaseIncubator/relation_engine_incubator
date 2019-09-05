"""
Classes for interfacing with the KBase relation engine database, specifically with graphs that
are versioned with batch time travelling (see 
https://github.com/kbaseIncubator/relation_engine_incubator/blob/master/delta_load_algorithm.md)

The classes and methods here were created for the purpose of supporting the graph delta loader, but
more classes and methods can be added as needed.

"""

# TODO CODE check id, from, and to for validity per https://www.arangodb.com/docs/stable/data-modeling-naming-conventions-document-keys.html

from arango.exceptions import AQLQueryExecuteError as _AQLQueryExecuteError
from arango.exceptions import DocumentDeleteError as _DocumentDeleteError

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

_FLD_RGSTR_LOAD_NAMESPACE = 'load_namespace'
_FLD_RGSTR_LOAD_VERSION = 'load_version'
_FLD_RGSTR_LOAD_TIMESTAMP = 'load_timestamp'
_FLD_RGSTR_VERTEX_COLLECTION = 'vertex_collection'
_FLD_RGSTR_MERGE_COLLECTION = 'merge_collection'
_FLD_RGSTR_EDGE_COLLECTIONS = 'edge_collections'
_FLD_RGSTR_START_TIME = 'start_time'
_FLD_RGSTR_COMPLETE_TIME = 'completion_time'
_FLD_RGSTR_STATE = 'state'
_FLD_RGSTR_STATE_IN_PROGRESS = 'in_progress'
_FLD_RGSTR_STATE_COMPLETE = 'complete'

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
            load_registry_collection,
            vertex_collection,
            default_edge_collection=None,
            edge_collections=None,
            merge_collection=None):
        """
        Create the DB interface.

        database - the python_arango ArangoDB database containing the data to query or modify.
        load_registry_collection - the name of the collection where loads will be listed.
        vertex_collection - the name of the collection to use for vertex operations.
        default_edge_collection - the name of the collection to use for edge operations by default.
          This can be overridden.
        edge_collections - a list of any edge collections in the graph.
          The collections are checked for existence and cached for performance reasons.
        merge_collection - a collection containing edges that indicate that a node has been 
          merged into another node.

        Specifying an edge collection in a method argument that is not in edge_collections,
        is not the default edge collection, or is not the merge collection will result in an error.
        """
        self._database = database
        self._merge_collection = None
        if merge_collection:
            self._merge_collection = self._init_col(merge_collection, edge=True)
        self._default_edge_collection = default_edge_collection
        edgecols = set()
        if default_edge_collection:
            edgecols.add(default_edge_collection)
        if edge_collections:
            edgecols.update(edge_collections)
        if not edgecols:
            raise ValueError("At least one edge collection must be specified")
        self._vertex_collection = self._init_col(vertex_collection)
        # TODO CODE could check if any loads are in progress for the namespace and bail if so
        self._registry_collection = self._init_col(load_registry_collection)

        self._edgecols = {n: self._init_col(n, edge=True) for n in edgecols}

        self._id_indexes = self._check_indexes()

    def _check_indexes(self):
        # check indexes and store names of required indexes
        cols = [self._vertex_collection] + list(self._edgecols.values())
        if self.get_merge_collection():
            cols.append(self._merge_collection)
        
        id_indexes = {}
        for col in cols:
            idx = col.indexes() # http request
            id_indexes[col.name] = self._get_index_name(col.name, self._ID_EXP_CRE_INDEX, idx)
            # check the other required index exists. Don't need to store it for later though
            self._get_index_name(col.name, self._EXP_CRE_LAST_VER_INDEX, idx)
        
        return id_indexes

    _ID_EXP_CRE_INDEX = {
        'type': 'persistent',
        'fields': [_FLD_ID, _FLD_EXPIRED, _FLD_CREATED],
        'sparse': False,
        'unique': False
        }

    _EXP_CRE_LAST_VER_INDEX = {
        'type': 'persistent',
        'fields': [_FLD_EXPIRED, _FLD_CREATED, _FLD_VER_LST],
        'sparse': False,
        'unique': False
    }

    def _get_index_name(self, col_name, index_spec, indexes):
        for idx in indexes:
            if not self._is_index_equivalent(index_spec, idx):
                continue
            return idx['name']
        raise ValueError(f'Collection {col_name} is missing required index with ' +
            f'specification {index_spec}')

    def _is_index_equivalent(self, index_spec, index):
        for field in index_spec:
            if index_spec[field] != index.get(field):
                return False
        return True

    # if an edge is inserted into a non-edge collection _from and _to are silently dropped
    def _init_col(self, collection, edge=False):
        c = self._database.collection(collection)
        if not c.properties()['edge'] is edge: # this is a http call
            ctype = 'an edge' if edge else 'a vertex'
            raise ValueError(f'{collection} is not {ctype} collection')
        return c

    def register_load_start(self, load_namespace, load_version, timestamp, current_time):
        """
        Register that a load is starting in the database.
        load_namespace - the unique namespace of the data set, e.g. NCBI_TAXA, GENE_ONTOLOGY,
          ENVO, etc.
        load_version - the version of the load that is unique within the namespace.
        timestamp - the timestamp in unix epoch milliseconds when the load will become active.
        current_time - the current time in unix epoch milliseconds.
        """
        doc = {_FLD_KEY: load_namespace + '_' + load_version,
               _FLD_RGSTR_START_TIME: current_time,
               _FLD_RGSTR_LOAD_NAMESPACE: load_namespace,
               _FLD_RGSTR_LOAD_VERSION: load_version,
               _FLD_RGSTR_LOAD_TIMESTAMP: timestamp,
               _FLD_RGSTR_COMPLETE_TIME: None,
               _FLD_RGSTR_STATE: _FLD_RGSTR_STATE_IN_PROGRESS,
               _FLD_RGSTR_VERTEX_COLLECTION: self._vertex_collection.name,
               _FLD_RGSTR_MERGE_COLLECTION: self.get_merge_collection(),
               _FLD_RGSTR_EDGE_COLLECTIONS: sorted(list(self._edgecols.keys()))}
        
        try:
            self._database.aql.execute(
                f'INSERT @d in @@col',
                bind_vars={'d': doc, '@col': self._registry_collection.name}
            )
        except _AQLQueryExecuteError as e:
            if e.error_code == 1210:
                raise ValueError('Load is already registered')
            raise e

    def register_load_complete(self, load_namespace, load_version, current_time):
        """
        Register that a load has completed in the database.
        load_namespace - the unique namespace of the data set, e.g. NCBI_TAXA, GENE_ONTOLOGY,
          ENVO, etc.
        load_version - the version of the load that is unique within the namespace.
        current_time - the current time in unix epoch milliseconds.
        """
        doc = {_FLD_KEY: load_namespace + '_' + load_version,
               _FLD_RGSTR_COMPLETE_TIME: current_time,
               _FLD_RGSTR_STATE: _FLD_RGSTR_STATE_COMPLETE}
        
        try:
            self._database.aql.execute(
                f'UPDATE @d in @@col',
                bind_vars={'d': doc, '@col': self._registry_collection.name}
            )
        except _AQLQueryExecuteError as e:
            if e.error_code == 1202:
                raise ValueError('Load is not registered, cannot be completed')
            raise e

    # TODO DOCS document fields
    # probably few enough of these that indexes aren't needed
    def get_registered_loads(self, load_namespace):
        """
        Returns all the registered loads for a namespace sorted by load timestamp from newest to
        oldest.

        load_namespace - the namespace of the loads to return.
        """
        cur = self._database.aql.execute(
            f"""
            FOR d in @@col
                FILTER d.{_FLD_RGSTR_LOAD_NAMESPACE} == @load_namespace
                SORT d.{_FLD_RGSTR_LOAD_TIMESTAMP} DESC
                return d
            """,
            bind_vars = {'load_namespace': load_namespace, '@col': self._registry_collection.name}
        )
        return [self._clean(d) for d in cur]

    def delete_registered_load(self, load_namespace, load_version):
        """
        Deletes a load from the registry.
        """
        try:
            self._registry_collection.delete({_FLD_KEY: load_namespace + '_' + load_version})
        except _DocumentDeleteError as e:
            if e.error_code == 1202:
                raise ValueError(f'There is no load version {load_version} ' +
                    f'in namespace {load_namespace}')

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
        edge collection, if any. Does not include the merge collection.
        """
        return sorted(list(self._edgecols.keys()))

    def get_merge_collection(self):
        """
        Return the name of the merge collection or None if no merge collection was registered.
        """
        # for some reason is None works, just a check doesn't
        return None if self._merge_collection is None else self._merge_collection.name

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
        id_idx = self._id_indexes[collection_name]
        cur = self._database.aql.execute(
          f"""
          FOR d IN @@col
              OPTIONS {{indexHint: @id_idx, forceIndexHint: true}}
              FILTER d.{_FLD_ID} IN @ids
              FILTER d.{_FLD_EXPIRED} >= @timestamp AND d.{_FLD_CREATED} <= @timestamp
              RETURN d
          """,
          bind_vars={'ids': ids, 'timestamp': timestamp, '@col': collection_name, 'id_idx': id_idx}
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
        Sets the expiration time on a vertex.

        key - the vertex key.
        expiration_time - the time, in Unix epoch milliseconds, to set as the expiration time
          on the vertex.
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
        col = self._vertex_collection
        self._expire_extant_document_without_last_version(timestamp, version, col)

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
        col = self._get_edge_collection(edge_collection)
        self._expire_extant_document_without_last_version(timestamp, version, col)
    
    def _expire_extant_document_without_last_version(self, timestamp, version, col):
        self._database.aql.execute(
            f"""
            FOR d IN @@col
                FILTER d.{_FLD_EXPIRED} >= @timestamp && d.{_FLD_CREATED} <= @timestamp
                FILTER d.{_FLD_VER_LST} != @version
                UPDATE d WITH {{{_FLD_EXPIRED}: @timestamp}} IN @@col
            """,
            bind_vars={'version': version, 'timestamp': timestamp, '@col': col.name},
        )

    # TODO PERF could add created index to speed this up
    def delete_created_documents(self, collection, creation_time):
        """
        Deletes any documents in the collection that were created at the given time.

        collection - the collection to modify.
        creation_time - the time of creation, in unix epoch milliseconds, of the documents to
          delete.
        """
        col = self._get_collection(collection) # ensure collection exists
        self._database.aql.execute(
            f"""
            FOR d IN @@col
                FILTER d.{_FLD_CREATED} == @timestamp
                REMOVE d IN @@col
            """,
            bind_vars={'timestamp': creation_time, '@col': col.name},
        )

    def undo_expire_documents(self, collection, expire_time):
        """
        Unexpires any documents that were expired at the given time.

        collection - the collection to modify
        expire_time - the time of expiration, in unix epoch milliseconds, of the documents to
          un-expire.
        """
        col = self._get_collection(collection) # ensure collection exists
        self._database.aql.execute(
            f"""
            FOR d IN @@col
                FILTER d.{_FLD_EXPIRED} == @timestamp
                UPDATE d WITH {{{_FLD_EXPIRED}: {_MAX_ADB_INTEGER}}} IN @@col
            """,
            bind_vars={'timestamp': expire_time, '@col': col.name},
        )

    # TODO PERF could add last_version index to speed this up
    def reset_last_version(self, collection, last_version, new_last_version):
        """
        Updates documents from one last version to another. Only documents with the given last
        version are affected.

        collection - the collection to modify
        last_version - any documents with this last_version will be modified.
        new_last_version - the documents will be modified to this last version.
        """
        col = self._get_collection(collection) # ensure collection exists
        self._database.aql.execute(
            f"""
            FOR d IN @@col
                FILTER d.{_FLD_VER_LST} == @last_version
                UPDATE d WITH {{{_FLD_VER_LST}: @new_last}} IN @@col
            """,
            bind_vars={
                'last_version': last_version,
                'new_last': new_last_version,
                '@col': col.name},
        )

    # mutates in place!
    def _clean(self, obj):
        for k in _INTERNAL_ARANGO_FIELDS:
            del obj[k] 
        return obj

    def _get_collection(self, collection):
        if self._vertex_collection.name == collection:
            return self._vertex_collection
        # again doesn't work without the is not None part. Dunno why.
        if self._merge_collection is not None and collection == self._merge_collection.name:
            return self._merge_collection
        if collection not in self._edgecols:
            raise ValueError(f'Collection {collection} was not registered at initialization')
        return self._edgecols[collection]

    def _get_edge_collection(self, collection):
        if not collection:
            if not self._default_edge_collection:
                raise ValueError('No default edge collection specified, ' +
                    'must specify edge collection')
            return self._edgecols[self._default_edge_collection]
        # again doesn't work without the is not None part. Dunno why.
        if self._merge_collection is not None and collection == self._merge_collection.name:
            return self._merge_collection
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
        self._ensure_vertex()
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
        self._ensure_edge()
        edge = _create_edge(id_, from_vertex, to_vertex, version, created_time, data)
        self._updates.append(edge)
        return edge[_FLD_KEY]

    def set_last_version_on_vertex(self, key, last_version):
        """
        Set the last version field on a vertex.

        key - the key of the vertex.
        last_version - the version to set.
        """
        self._ensure_vertex()
        self._updates.append({_FLD_KEY: key, _FLD_VER_LST: last_version})

    def set_last_version_on_edge(self, edge, last_version):
        """
        Set the last version field on an edge.

        edge - the edge to update. This must have been fetched from the database.
        last_version - the version to set.
        """
        self._update_edge(edge, {_FLD_VER_LST: last_version})
    
    def _update_edge(self, edge, update):
        self._ensure_edge()
        update[_FLD_KEY] = edge[_FLD_KEY]
        # this is really lame. Arango requires the _to and _from edges even when the
        # document you're updating already has them.
        update[_FLD_FROM] = edge[_FLD_FROM]
        update[_FLD_TO] = edge[_FLD_TO]
        self._updates.append(update)

    def expire_vertex(self, key, expiration_time):
        """
        Sets the expiration time on a vertex.

        key - the vertex key.
        expiration_time - the time, in Unix epoch milliseconds, to set as the expiration time
          on the vertex.
        """
        self._ensure_vertex()
        self._updates.append({_FLD_KEY: key, _FLD_EXPIRED: expiration_time})

    def expire_edge(self, edge, expiration_time):
        """
        Sets the expiration time on an edge.

        edge - the edge to update. This must have been fetched from the database.
        expiration_time - the time, in Unix epoch milliseconds, to set as the expiration time
          on the edge.
        """
        self._update_edge(edge, {_FLD_EXPIRED: expiration_time})

    def update(self):
        """
        Apply the updates collected so far and clear the update list.
        """
        self._col.import_bulk(self._updates, on_duplicate="update")
        self._updates.clear()

    def count(self):
        """
        Get the number of pending updates.
        """
        return len(self._updates)

    def _ensure_vertex(self):
        if self.is_edge:
            raise ValueError('Batch updater is configured for an edge collection')

    def _ensure_edge(self):
        if not self.is_edge:
            raise ValueError('Batch updater is configured for a vertex collection')

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