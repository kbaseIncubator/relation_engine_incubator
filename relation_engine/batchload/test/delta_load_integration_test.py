# Tests the delta load algorithm using an arangodb database.
# As such, the tests are fairly complex.

# This does not test the database wrapper code - it has its own tests.

# TODO TEST start a new arango instance as part of the tests so:
# a) we remove chance of data corruption and 
# b) we don't leave test data around

# TODO TEST add unit tests for the delta load algorithm with a db mock.

from relation_engine.batchload.time_travelling_database import ArangoBatchTimeTravellingDB
from relation_engine.batchload.delta_load import load_graph_delta
from arango import ArangoClient
from pytest import fixture

HOST = 'localhost'
PORT = 8529
DB_NAME = 'test_delta_load_integration_db'

ADB_MAX_TIME = 2**53 - 1

@fixture
def arango_db():
    client = ArangoClient(protocol='http', host=HOST, port=PORT)
    sys = client.db('_system', 'root', '', verify=True)
    sys.delete_database(DB_NAME, ignore_missing=True)
    sys.create_database(DB_NAME)
    db = client.db(DB_NAME)

    yield db

    sys.delete_database(DB_NAME)

def test_merge_setup_fail(arango_db):
    """
    Tests that the algorithm fails to start if a merge source is specified but a merge collection
    is not
    """
    arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)

    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')

    # sources are fake, but real not necessary to trigger error
    _check_exception(lambda: load_graph_delta([], [], att, 1, "2", merge_source=[{}]),
        ValueError, 'A merge source is specified but the database has no merge collection')

def test_load_no_merge_source_batch_2(arango_db):
    _load_no_merge_source(arango_db, 2)

def test_load_no_merge_source_batch_default(arango_db):
    _load_no_merge_source(arango_db, None)

def _load_no_merge_source(arango_db, batchsize):
    """
    Test delta loading a small graph, including deleted, updated, unchanged, and new nodes and
    edges.
    """
    vcol = arango_db.create_collection('v')
    def_ecol = arango_db.create_collection('def_e', edge=True)
    e1col = arango_db.create_collection('e1', edge=True)
    e2col = arango_db.create_collection('e2', edge=True)

    # there are 2 update and 2 same nodes for the purposes of testing edge updates correctly
    _import_bulk(
        vcol,
        [
         {'id': 'old', 'data': 'foo'},            # will be deleted
         {'id': 'same1', 'data': {'bar': 'baz'}}, # will not change
         {'id': 'same2', 'data': ['bar', 'baz']}, # will not change
         {'id': 'up1', 'data': {'old': 'data'}},  # will be updated
         {'id': 'up2', 'data': ['old', 'data']}   # will be updated
        ],
        100, ADB_MAX_TIME, 'v1')

    _import_bulk(
        def_ecol,
        [
         {'id': 'old', 'from': 'old', 'to': 'up1', 'data': 'foo'},  # will be deleted
         {'id': 'up1', 'from': 'same1', 'to': 'up1', 'data': 'bar'} # will be updated to new up1
        ],
        100, ADB_MAX_TIME, 'v1', vert_col_name=vcol.name)

    _import_bulk(
        e1col,
        [
         {'id': 'old', 'from': 'old', 'to': 'same1', 'data': 'baz'},    # will be deleted
         {'id': 'same', 'from': 'same1', 'to': 'same2', 'data': 'bing'} # no change
        ],
        100, ADB_MAX_TIME, 'v1', vert_col_name=vcol.name)

    _import_bulk(
        e2col,
        [
         {'id': 'change', 'from': 'same1', 'to': 'same2', 'data': 'baz'}, # will be updated
         {'id': 'up2', 'from': 'up2', 'to': 'same2', 'data': 'boof'}      # will be updated to up2
        ],
        100, ADB_MAX_TIME, 'v1', vert_col_name=vcol.name)

    vsource = [
        {'id': 'same1', 'data': {'bar': 'baz'}}, # will not change
        {'id': 'same2', 'data': ['bar', 'baz']}, # will not change
        {'id': 'up1', 'data': {'new': 'data'}},  # will be updated based on data
        {'id': 'up2', 'data': ['old', 'data1']}, # will be updated based on data
        {'id': 'new', 'data': 'super sweet'}     # new node
    ]

    esource = [
        # will be updated since up1 is updated. Default collection.
        {'id': 'up1', 'from': 'same1', 'to': 'up1', 'data': 'bar'},
        # won't change
        {'_collection': 'e1', 'id': 'same', 'from': 'same1', 'to': 'same2', 'data': 'bing'},
        # will be updated based on data
        {'_collection': 'e2', 'id': 'change', 'from': 'same1', 'to': 'same2', 'data': 'boo'},
        # will be updated since up2 is updated.
        {'_collection': 'e2', 'id': 'up2', 'from': 'up2', 'to': 'same2', 'data': 'boof'},
        # new edge
        {'_collection': 'def_e', 'id': 'new', 'from': 'new', 'to': 'same1', 'data': 'new'}
    ]

    db = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='def_e',
            edge_collections=['e1', 'e2'])
    
    if batchsize:
        load_graph_delta(vsource, esource, db, 500, 'v2', batch_size=batchsize)
    else: 
        load_graph_delta(vsource, esource, db, 500, 'v2')

    vexpected = [
        {'id': 'old', '_key': 'old_v1', '_id': 'v/old_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'foo'},
        {'id': 'same1', '_key': 'same1_v1', '_id': 'v/same1_v1',
         'first_version': 'v1', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'data': {'bar': 'baz'}},
        {'id': 'same2', '_key': 'same2_v1', '_id': 'v/same2_v1',
         'first_version': 'v1', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'data': ['bar', 'baz']},
        {'id': 'up1', '_key': 'up1_v1', '_id': 'v/up1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': {'old': 'data'}},
        {'id': 'up1', '_key': 'up1_v2', '_id': 'v/up1_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': {'new': 'data'}},
        {'id': 'up2', '_key': 'up2_v1', '_id': 'v/up2_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': ['old', 'data']},
        {'id': 'up2', '_key': 'up2_v2', '_id': 'v/up2_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': ['old', 'data1']},
        {'id': 'new', '_key': 'new_v2', '_id': 'v/new_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'super sweet'}
    ]

    _check_docs(arango_db, vexpected, 'v')

    def_e_expected = [
        {'id': 'old', 'from': 'old', 'to': 'up1',
         '_key': 'old_v1', '_id': 'def_e/old_v1', '_from': 'v/old_v1', '_to': 'v/up1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'foo'},
        {'id': 'up1', 'from': 'same1', 'to': 'up1',
         '_key': 'up1_v1', '_id': 'def_e/up1_v1', '_from': 'v/same1_v1', '_to': 'v/up1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'bar'},
        {'id': 'up1', 'from': 'same1', 'to': 'up1',
         '_key': 'up1_v2', '_id': 'def_e/up1_v2', '_from': 'v/same1_v1', '_to': 'v/up1_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'bar'},
        {'id': 'new', 'from': 'new', 'to': 'same1',
         '_key': 'new_v2', '_id': 'def_e/new_v2', '_from': 'v/new_v2', '_to': 'v/same1_v1',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'new'}
    ]

    _check_docs(arango_db, def_e_expected, 'def_e')

    e1_expected = [
        {'id': 'old', 'from': 'old', 'to': 'same1',
         '_key': 'old_v1', '_id': 'e1/old_v1', '_from': 'v/old_v1', '_to': 'v/same1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'baz'},
        {'id': 'same', 'from': 'same1', 'to': 'same2',
         '_key': 'same_v1', '_id': 'e1/same_v1', '_from': 'v/same1_v1', '_to': 'v/same2_v1',
         'first_version': 'v1', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'data': 'bing'},
    ]

    _check_docs(arango_db, e1_expected, 'e1')

    e2_expected = [
        {'id': 'change', 'from': 'same1', 'to': 'same2',
         '_key': 'change_v1', '_id': 'e2/change_v1', '_from': 'v/same1_v1', '_to': 'v/same2_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'baz'},
        {'id': 'change', 'from': 'same1', 'to': 'same2',
         '_key': 'change_v2', '_id': 'e2/change_v2', '_from': 'v/same1_v1', '_to': 'v/same2_v1',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'boo'},
        {'id': 'up2', 'from': 'up2', 'to': 'same2',
         '_key': 'up2_v1', '_id': 'e2/up2_v1', '_from': 'v/up2_v1', '_to': 'v/same2_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'boof'},
        {'id': 'up2', 'from': 'up2', 'to': 'same2',
         '_key': 'up2_v2', '_id': 'e2/up2_v2', '_from': 'v/up2_v2', '_to': 'v/same2_v1',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'boof'},
    ]

    _check_docs(arango_db, e2_expected, 'e2')

# modifies docs in place!
# vert_col_name != None implies and edge
def _import_bulk(col, docs, created, expired, version, vert_col_name=None):
    for d in docs:
        d['_key'] = d['id'] + '_' + version
        if vert_col_name:
            d['_from'] = vert_col_name + '/' + d['from'] + '_' + version
            d['_to'] = vert_col_name + '/' + d['to'] + '_' + version
        d['created'] = created
        d['expired'] = expired
        d['first_version'] = version
        d['last_version'] = version
    col.import_bulk(docs)

def _check_exception(action, exception, message):
    try:
        action()
        assert 0, "Expected exception"
    except exception as e:
        assert e.args[0] == message

def _check_docs(arango_db, docs, collection):
    col = arango_db.collection(collection)
    assert col.count() == len(docs), 'Incorrect # of docs in collection ' + collection
    for d in docs:
        doc = col.get(d['_key'])
        del doc['_rev']
        assert d == doc