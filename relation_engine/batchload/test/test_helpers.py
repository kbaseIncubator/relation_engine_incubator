def check_exception(action, exception, message):
    """
    Checks that an exception is thrown as expected.

    action - a callable that is expected to throw an exception.
    exception - the exception class.
    message - the exception message.
    """

    try:
        action()
        assert 0, "Expected exception"
    except exception as e:
        assert e.args[0] == message

def check_docs(arango_db, docs, collection):
    """
    Check that an ArangoDB collection contains the specified documents. Ignores the _rev key.

    arango_db - the arango database containing the collection.
    docs - the expected collection contents.
    collection - the name of the collection.
    """

    col = arango_db.collection(collection)
    assert col.count() == len(docs), 'Incorrect # of docs in collection ' + collection
    for d in docs:
        doc = col.get(d['_key'])
        del doc['_rev']
        assert d == doc


def create_timetravel_collection(arango_db, name, edge=False):
    """
    Creates an ArangoDB collection with appropriate indexes for a time travelling schema.

    arango_db - the database that will contain the collection.
    name - the name of the collection.
    edge - True for an edge collection (default False).

    Returns a python-arango collection instance.
    """

    col = arango_db.create_collection(name, edge=edge)
    col.add_persistent_index(['id', 'expired', 'created'])
    col.add_persistent_index(['expired', 'created', 'last_version'])
    return col