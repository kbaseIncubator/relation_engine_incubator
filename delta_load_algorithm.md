# Graph delta load algorithm

This document explains the load algorithm KBase uses for loading new versions of graph data
(at the time of writing, this is targeted towards taxonomy and ontology graphs) as a batch into a
graph database already containing one or more previous versions of the data.

KBase indends to use ArangoDB for graph queries and so many of the conventions for field names,
etc., originate there.

## Assumptions

* Only one instance of the loader runs at once. Unspecified behavior may result otherwise.

## Issues

* With the algorithm described, any queries that are performed while a load is in progress
  are not repeatable as the database is in an inconsistent state.
  * A potential fix is using a red-green scheme during the update process, although this adds
    significant complexity to the update process:
    * Create new nodes and edges collections (red).
    * Blacklist the new collections from any queries.
    * Copy the current collections (green) into the red collections.
    * Perform the update on the red collections.
    * Updating edges from external collections can proceed in one of two ways:
      * Halting
        * Halt updates to external collections
        * Copy all current edges to the new collection
        * Resume updates when the red -> green switch occurs (below)
      * Dual update
        * Add new edges to both the green and red collections
        * This runs the risk of leaving the db in an inconsistent state if an update fails
          for one collection but not the other
          * Need to consider how to restart the update from a case like this
          * Which collection gets the update first?
        * Stop adding updates to the green collection after the red -> green switch
    * When updates are complete, blacklist the green collections and remove the blacklist for
      the red collections
      * Ideally atomically - in ElasticSearch this is possible with aliases, for example
    * Delete the green collections
    * Change the red collection to green

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
    * What if a node is merged, unmerged, and merged again
      * Do we add merged edges for both cases? How do we know when the merge actually occurred
        without reconstructing the entire graph history every load?
  * If tax dumps are skipped, merging become even more complex

## Fields

### Nodes
* `_key` defines the unique ID of the node.
* `id` defines the ID of the node. This is usually the ID of the node from the external data
  source (e.g. a taxid for NCBI taxonomy). This ID is not guaranteed to be unique within the graph,
  but is unique for a particular version of the graph.
* `version` is a string that denotes in which version the node first appeared.
* `created` denotes the date, in unix epoch milliseconds, when the node began to exist.
* `expired`  denotes the date, in unix epoch milliseconds, when the node ceased to exist.

### Edges
* `_from` uniquely defines the node from which the edge originates. It contains a `_key` value.
* `_to` uniquely defines the node where the edge terminates. It contains a `_key` value.
* `from` contains the node `id` from which the edge originates.
* `to` contains the node `id` where the edge terminates.
* `version` is a string that denotes in which version the node first appeared.
* `created` denotes the date, in unix epoch milliseconds, when the edge began to exist.
* `expired`  denotes the date, in unix epoch milliseconds, when the edge ceased to exist.
* `type` denotes the type of the edge - either `std` for an edge which is part of the taxonomy or
  ontology (e.g the graph proper) or `merge` for an edge that denotes that the `from` node has been
  merged into the `to` node (and therefore the `from` node **must** be in a deleted state).
* In ArangoDB, the `?from` and `?to` fields are prefixed by the name of the collection in which the
  node resides.

## Algorithm

### Inputs

|Input|Description|
|-----|-----------|
|`nodes_source_file`|a file containing node data|
|`edges_source_file`|a file containing edge data|
|`merge_source_fild`|a file containg merged nodes data|
|`timestamp`|the timestamp (as the unix epoch in milliseconds) to use as the created date for the new nodes and edges. `timestamp` - 1 will be used for the expired date for deleted or merged nodes and edges.|
|`version`|the version of the load. This is applied to nodes and edges for informational purposes, and used to create unique `_key`s from node IDs.|
                      

### Algorithm

```

def delete_node(nodekey):
    # this may be possible in a single db update
    for edge in get_edges_for_node_from_db(nodekey, timestamp):
        set_edge_expiration_in_db(edge._from, edge._to, timestamp - 1)
    set_node_expiration_in_db(nodekey, timestamp - 1)
      
def create_node(node):
    node._key = generate_key(...)
    node.version = version
    node.created = timestamp
    node.expired = ∞
    save_node_in_db(node)

def create_edge(merged_node, merged_into_node, edge)
    edge._from: merged_node._key
    edge._to: merged_into_node._key
    edge.from: merged_node.id
    edge.to: merged_into_node.id
    edge.version: version
    edge.created: timestamp
    edge.expired: ∞
    save_edge_in_db(edge)

def main():
    extant_nodes = set()              # if there are many nodes this may need to be stored in a db
    for node in get_nodes(nodes_source_file):
        extant_nodes.add(node.id)
        existing = get_node_from_db(node.id, timestamp)
        if not existing:
            create_node(node)
        elif existing != node:
            delete_node(existing._key)
            create_node(node)

    # for merges, we only consider merges that occurred in the current release - e.g if either
    # node doesn't exist, we ignore the merge. It starts getting complicated otherwise.
    if merge_source_file:
        for merged_id, merged_into_id in get_merges(merge_source_file):
            merged = get_node_from_db(merged_id, timestamp)
            merged_into = get_node_from_db(merged_into_id, timestamp)
            if merged and merged_into:
                # don't need to check whether the edge exists because the algorithm will never
                # leave a node undeleted with an outgoing merge edge. If both nodes exist,
                # there's no prexisting edge.
                delete_node(merged._key)
                create_edge(merged, merged_into, {type: merge})
        
    # since not all sources of graphs possess delete info, we figure it out ourselves
    for node in find_extant_nodes_in_db(timestamp):
        if node.id not in extant_nodes:
            delete_node(node._key)
      

for edge in get_edges(edges_source_file)
    from = get_node_from_db(edge.from, timestamp)
    to = get_node_from_db(edge.to, timestamp)

    existing = get_edge_from_db(from_key, to_key)
    edge.type = std
    if not existing:
        create_edge(from, to, edge)
    elif existing != edge:
        set_edge_expiration_in_db(edge._from, edge._to, timestamp - 1)
        create_edge(from, to, edge)
```

### Notes
* Node and edge equality does not include the `_key`, `_to`, `_from`, `created`, `expired`, and
  `version` fields, but does include any other fields defined on the node or edge as part of the 
  graph schema.
* `generate_key(...)` could have a number of different implementations:
  * Completely random, like a UUID
  * node.id + '\_' + version
  * node.id + a UUID per load
  * node.id + a user specified suffix per load
  * The only requirement is that it is guaranteed to be unique
  * As of 2019-7-23 node.id + '\_' + version was chosen

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