# Graph delta load algorithm

This document explains the load algorithm KBase uses for loading new versions of graph data
(at the time of writing, this is targeted towards taxonomy and ontology graphs) into a graph
database already containing one or more previous versions of the data.

KBase indends to use ArangoDB for graph queries and so many of the conventions for field names,
etc., originate there.

## Assumptions

* Only one instance of the loader runs at once. Unspecified behavior may result otherwise.

## Limitations

* This algorithm does not address relinking data that is linked to the graph, but not part of
  the graph, to merged nodes. It does not even detect merged nodes.
  * For example, linking genomes that were linked into a taxonomy graph to a new node if the
    current node is merged.

## Fields

### Nodes
* `_key` defines the unique ID of the node.
* `id` defines the ID of the node. This is usually the ID of the node from the external data
  source (e.g. a taxid for NCBI taxonomy). This ID is not guaranteed to be unique within the graph,
  but is unique for a particular version of the graph.
* `versions` is an array of strings that define in which versions of the graph the node exists.

### Edges
* `_from` uniquely defines the node from which the edge originates. It contains a `_key` value.
* `_to` uniquely defines the node where the edge terminates. It contains a `_key` value.
* `from` contains the node `id` from which the edge originates.
* `to` contains the node `id` where the edge terminates.
* `versions` is an array of strings that define in which versions of the graph the edge exists.
* In ArangoDB, the `?from` and `?to` fields are prefixed by the name of the collection in which the
  node resides.

In addition, the `versions` of the graph must be listed, in load order, somewhere independently
of the nodes and edges so that the most recent version (or all the versions if, for example, a
user will select a version) can be retrieved for the purposes of querying the database.

## Algorithm

### Inputs
`nodes_source_file` = a file containing node data.  
`edges_source_file` = a file containing edge data.  
`version` = the version of the load.

### Algorithm
```
for node in get_nodes(nodes_source_file):
    existing_nodes = get_nodes_from_db_in_version_order(node.id)
    for en in existing_nodes:
        if node == en:
            add_version_to_node_in_db(en._key, version)
            continue
    # new node
    node._key = generate_key(...)
    node.versions = [version]
    save_node_in_db(node)

for edge in get_edges(edges_source_file)
    from_key = get_node_key_from_db(edge.from, version)
    to_key = get_node_key_from_db(edge.to, version)

    existing_edge = get_edge_from_db(from_key, to_key)
    if not existing_edge or existing_edge != edge:
        edge._from = from_key
        edge._to = to_key
        edge.versions = [version]
        save_edge_in_db(edge)
    else:
        add_version_to_edge_in_db(existing_edge._from, existing_edge._to)

push_version_to_version_collection(version) # adds to top of version list
```

### Notes
* Node and edge equality does not include the `_key`, `_to`, `_from`, and `version` fields, but
  does include any other fields defined on the node or edge as part of the graph schema.
* `generate_key(...)` could have a number of different implementations:
  * Completely random, like a UUID
  * node.id + '\_' + version
  * node.id + a UUID per load
  * node.id + a user specified suffix per load
  * The only requirement is that it is guaranteed to be unique

### Speed

* To speed up the algorithm, the nodes and edges could be split into parts and parallelized.
  * The nodes must all be loaded prior to the edges to establish the `_key` field.
    * Unless you really want to make the algorithm complex.

## TODO

* Handle [time traveling](https://www.arangodb.com/2018/07/time-traveling-with-graph-databases/)