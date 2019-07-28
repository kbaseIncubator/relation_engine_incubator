# Graph delta load algorithm

This document explains the load algorithm KBase uses for loading new versions of graph data
(at the time of writing, this is targeted towards taxonomy and ontology graphs) as a batch into a
graph database already containing one or more previous versions of the data.

KBase indends to use ArangoDB for graph queries and so many of the conventions for field names,
etc., originate there.

## Assumptions

* Only one instance of the loader runs at once. Unspecified behavior may result otherwise.
* There is only one extant node per external ID (e.g. per NCBI Taxonomy ID) at any one time.
* There is only one extant edge between any two extant nodes at any one time.

## Issues

* With the algorithm described, any queries that are performed while a load is in progress
  are not repeatable as the database is in an inconsistent state.
  * See delta_load_atomicity.md for potential solutions.

## Limitations

### Relinking

* This algorithm does not address relinking data that is linked to the graph, but not part of
  the graph, to merged nodes.
  * For example, linking genomes that were linked into a taxonomy graph to a new node if the
    current node is merged.

### Merges

* The algorithm only considers merges that occurred in the current load delta.
  * NCBI merge data is cumulative and does not specify when a merge occurred.
  * The algorithm expects that both nodes exist in the prior load.
    * This means that deleted -> merged transitions (which occur in NCBI) will be ignored
  * Taking deleted nodes into account becomes complex
    * Nodes can transition from merged -> deleted or vice versa
    * Need to check for merge edges in previously deleted nodes
    * What if a node is merged, deleted, and merged again
      * Do we add merged edges for both cases? How do we know when the merge actually occurred
        without reconstructing the entire graph history every load?
  * If tax dumps are skipped, merging become even more complex

## Fields

### Nodes
* `_key` defines the unique ID of the node.
* `id` defines the ID of the node. This is usually the ID of the node from the external data
  source (e.g. a taxid for NCBI taxonomy). This ID is not guaranteed to be unique within the graph,
  but is unique for a particular version of the graph.
* `first_version` is a string that denotes in which version the node first appeared.
* `last_version` is a string that denotes in which version the node last appeared.
* `created` denotes the date, in unix epoch milliseconds, when the node began to exist.
* `expired`  denotes the date, in unix epoch milliseconds, when the node ceased to exist.

### Edges
* `_from` uniquely defines the node from which the edge originates. It contains a `_key` value.
* `_to` uniquely defines the node where the edge terminates. It contains a `_key` value.
* `from` contains the node `id` from which the edge originates.
* `to` contains the node `id` where the edge terminates.
* `first_version` is a string that denotes in which version the edge first appeared.
* `last_version` is a string that denotes in which version the edge last appeared.
* `created` denotes the date, in unix epoch milliseconds, when the edge began to exist.
* `expired`  denotes the date, in unix epoch milliseconds, when the edge ceased to exist.
* `type` denotes the type of the edge - either `std` for an edge which is part of the taxonomy or
  ontology (e.g the graph proper) or `merge` for an edge that denotes that the `from` node has been
  merged into the `to` node (and therefore the `from` node **must** be expired).
* In ArangoDB, the `?from` and `?to` fields are prefixed by the name of the collection in which the
  node resides.

## Algorithm

### Inputs

|Input|Description|
|-----|-----------|
|`nodes_source`|a source of node data|
|`edges_source`|a source of edge data|
|`merge_source`|a source of merged nodes data|
|`timestamp`|the timestamp (as the unix epoch in milliseconds) to use as the created date for the new nodes and edges. `timestamp` - 1 will be used for the expired date for deleted or merged nodes and edges.|
|`version`|the version of the load. This is applied to nodes and edge and used to create unique `_key`s from node IDs.|
                      

### Algorithm

```

def delete_node(nodekey):
    # this may be possible in a single db update
    for edge in get_edges_for_node_from_db(nodekey, timestamp):
        set_edge_expiration_in_db(edge._from, edge._to, timestamp - 1)
    set_node_expiration_in_db(nodekey, timestamp - 1)
      
def create_node(node):
    node._key = generate_key(...)
    node.first_version = version
    node.last_version = version
    node.created = timestamp
    node.expired = ∞
    save_node_in_db(node)

def create_edge(merged_node, merged_into_node, edge)
    edge._from: merged_node._key
    edge._to: merged_into_node._key
    edge.from: merged_node.id
    edge.to: merged_into_node.id
    edge.first_version: version
    edge.last_version: version
    edge.created: timestamp
    edge.expired: ∞
    save_edge_in_db(edge)

def main():
    for node in get_nodes(nodes_source):
        existing = get_node_from_db(node.id, timestamp)
        if not existing:
            create_node(node)
        elif existing != node:
            # for continous data you'd need the deletion of the node & edges and creation
            # of the new node and edges to be a transaction.
            # Since this is a batch load we don't worry about it. The whole load would have
            # to be a transaction to make the db stable during the load.
            delete_node(existing._key)
            create_node(node)
        else:
            set_last_node_version_in_db(node._key, version) # mark node as extant in current load

    # For merges, we only consider merges that occurred in the current release - e.g if either
    # node doesn't exist, we ignore the merge. It starts getting complicated otherwise.
    # It is assumed the set of nodes from get_nodes() and the set of nodes from get_merges() are
    # disjoint.
    if merge_source:
        for merged_id, merged_into_id in get_merges(merge_source):
            merged = get_node_from_db(merged_id, timestamp)
            merged_into = get_node_from_db(merged_into_id, timestamp)
            if merged and merged_into:
                # don't need to check whether the edge exists because the algorithm will never
                # leave a node undeleted with an outgoing merge edge. If both nodes exist,
                # there's no preexisting edge.
                delete_node(merged._key)
                create_edge(merged, merged_into, {type: merge})
        
    # since not all sources of graphs possess delete info, we figure it out ourselves
    # may be possible to do this in one query
    for node in find_extant_nodes_without_last_version_in_db(timestamp, version):
        delete_node(node._key)
      

    for edge in get_edges(edges_source)
        from = get_node_from_db(edge.from, timestamp)
        to = get_node_from_db(edge.to, timestamp)

        # assumes there is only one extant edge from one node to another
        existing = get_edge_from_db(from._key, to._key, timestamp)
        edge.type = std
        if not existing:
            create_edge(from, to, edge)
        elif existing != edge:
            set_edge_expiration_in_db(existing._from, existing._to, timestamp - 1)
            create_edge(from, to, edge)
        else:
            # mark edge as extant in current load
            set_last_edge_version_in_db(existing._from, existing._to, timestamp, version)
    
    # If a node's edges are changed but the node is otherwise unchanged, the old edges will not
    # be deleted, so we need to delete any edges that aren't in the current version but still
    # exist
    # May be possible to do this in one query
    for edge in find_extant_edges_without_last_version_in_db(timestamp, version):
        set_edge_expiration_in_db(existing._from, existing._to, timestamp - 1)
```

### Notes
* Node and edge equality does not include the `_key`, `_to`, `_from`, `created`, `expired`, and
  `*_version` fields, but does include any other fields defined on the node or edge as part of the 
  graph schema.
* `generate_key(...)` could have a number of different implementations:
  * Completely random, like a UUID
  * node.id + '\_' + version
  * node.id + a UUID per load
  * node.id + a user specified suffix per load
  * The only requirement is that it is guaranteed to be unique
  * As of 2019-7-23 node.id + '\_' + version was chosen

### Indexes:

The following indexes are needed for decent performance of the delta loader & node / edge finding
and traversal queries. Multiple fields separated by a comma indicate a compound index.

Note that unique doesn't suggest that the index should be made unique in the database, as that
can prevent sharding.

#### Nodes:

|Index|Purpose|Unique?|
|-----|-------|-------|
|_key|default|Yes|
|id, created, expired|find nodes via an external ID and a timestamp. Used to locate prior node when updating a node.|Yes|
|created, expired, last_version|find extant nodes with or without a particular load version. Used to expire extant nodes not in the current load.|No|

#### Edges
|Index|Purpose|Unique?|
|-----|-------|-------|
|_from|default|No|
|_to|default|No|
|_from, _to, created, expired|find an edge exactly without having to travese all of a node's edges.|Yes*|
|created, expired, last_version|find extant edges with or without a particular load version. Used to expire extant edges not in the current load.|No|
|_from, created, expired|traverse downstream from a node given a timestamp.|No|
|_to, created, expired|traverse upstream from a node given a timestamp.|No|

* Merge edges should never co-exist with extant standard edges.

May also want indexes on `to` and `from` but they are not necessary for the delta loader or
traversals.


### Speed

* To speed up the algorithm
  * The nodes and edges could be split into parts and parallelized.
    * The nodes must all be loaded prior to the edges to establish the `_key` field.
      * Unless you really want to make the algorithm complex.
  * The new taxonomy could be loaded into a temporary DB and then DB queries used to update
    the real DB based on comparison to the temporary DB.
    * In the case of ArangoDB, Foxx might be useful.

### Other implementation nodes

* We do not use the proxy vertexes described in the
  [time travelling article](https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/)
  as we have very few large nodes and updates are expected to be as a batch, not continuous.
  This may need to change in the future.