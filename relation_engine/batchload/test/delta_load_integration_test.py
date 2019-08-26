# Tests the delta load algorithm using an arangodb database.
# As such, the tests are fairly complex.

# This does not test the database wrapper code - it has its own tests.

# TODO TEST start a new arango instance as part of the tests so:
# a) we remove chance of data corruption and 
# b) we don't leave test data around

# TODO TEST add unit tests for the delta load algorithm with a db mock.

# TODO NOW TEST merge tests

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

    _import_bulk(
        vcol,
        [
         {'id': 'expire', 'data': 'foo'},     # expired nodes shouldn't be touched
         {'id': 'gap', 'data': 'super sweet'}, # even if reintroduced later
        ],
        100, 300, 'v0')

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
        100, ADB_MAX_TIME, 'v0', 'v1')

    _import_bulk(
        def_ecol,
        [
         {'id': 'expire', 'from': 'expire', 'to': 'same2', 'data': 'foo'},  # shouldn't be touched
         {'id': 'gap', 'from': 'gap', 'to': 'same1', 'data': 'bar'}         # ditto
        ],
        100, 300, 'v0', vert_col_name=vcol.name)

    _import_bulk(
        def_ecol,
        [
         {'id': 'old', 'from': 'old', 'to': 'up1', 'data': 'foo'},  # will be deleted
         {'id': 'up1', 'from': 'same1', 'to': 'up1', 'data': 'bar'} # will be updated to new up1
        ],
        100, ADB_MAX_TIME, 'v0', 'v1', vert_col_name=vcol.name)

    _import_bulk(
        e1col,
        [
         {'id': 'old', 'from': 'old', 'to': 'same1', 'data': 'baz'},    # will be deleted
         {'id': 'same', 'from': 'same1', 'to': 'same2', 'data': 'bing'} # no change
        ],
        100, ADB_MAX_TIME, 'v0', 'v1', vert_col_name=vcol.name)

    _import_bulk(
        e2col,
        [
         {'id': 'change', 'from': 'same1', 'to': 'same2', 'data': 'baz'}, # will be updated
         {'id': 'up2', 'from': 'up2', 'to': 'same2', 'data': 'boof'}      # will be updated to up2
        ],
        100, ADB_MAX_TIME, 'v0', 'v1', vert_col_name=vcol.name)

    vsource = [
        {'id': 'same1', 'data': {'bar': 'baz'}}, # will not change
        {'id': 'same2', 'data': ['bar', 'baz']}, # will not change
        {'id': 'up1', 'data': {'new': 'data'}},  # will be updated based on data
        {'id': 'up2', 'data': ['old', 'data1']}, # will be updated based on data
        {'id': 'gap', 'data': 'super sweet'}     # new node
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
        {'_collection': 'def_e', 'id': 'gap', 'from': 'gap', 'to': 'same1', 'data': 'bar'}
    ]

    db = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='def_e',
            edge_collections=['e1', 'e2'])
    
    if batchsize:
        load_graph_delta(vsource, esource, db, 500, 'v2', batch_size=batchsize)
    else: 
        load_graph_delta(vsource, esource, db, 500, 'v2')

    vexpected = [
        {'id': 'expire', '_key': 'expire_v0', '_id': 'v/expire_v0',
         'first_version': 'v0', 'last_version': 'v0', 'created': 100, 'expired': 300,
         'data': 'foo'},
        {'id': 'gap', '_key': 'gap_v0', '_id': 'v/gap_v0',
         'first_version': 'v0', 'last_version': 'v0', 'created': 100, 'expired': 300,
         'data': 'super sweet'},
        {'id': 'gap', '_key': 'gap_v2', '_id': 'v/gap_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'super sweet'},
        {'id': 'old', '_key': 'old_v0', '_id': 'v/old_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'foo'},
        {'id': 'same1', '_key': 'same1_v0', '_id': 'v/same1_v0',
         'first_version': 'v0', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'data': {'bar': 'baz'}},
        {'id': 'same2', '_key': 'same2_v0', '_id': 'v/same2_v0',
         'first_version': 'v0', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'data': ['bar', 'baz']},
        {'id': 'up1', '_key': 'up1_v0', '_id': 'v/up1_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': {'old': 'data'}},
        {'id': 'up1', '_key': 'up1_v2', '_id': 'v/up1_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': {'new': 'data'}},
        {'id': 'up2', '_key': 'up2_v0', '_id': 'v/up2_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': ['old', 'data']},
        {'id': 'up2', '_key': 'up2_v2', '_id': 'v/up2_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': ['old', 'data1']},
    ]

    _check_docs(arango_db, vexpected, 'v')

    def_e_expected = [
        {'id': 'expire', 'from': 'expire', 'to': 'same2',
         '_key': 'expire_v0', '_id': 'def_e/expire_v0', '_from': 'v/expire_v0', '_to': 'v/same2_v0',
         'first_version': 'v0', 'last_version': 'v0', 'created': 100, 'expired': 300,
         'data': 'foo'},
        {'id': 'gap', 'from': 'gap', 'to': 'same1',
         '_key': 'gap_v0', '_id': 'def_e/gap_v0', '_from': 'v/gap_v0', '_to': 'v/same1_v0',
         'first_version': 'v0', 'last_version': 'v0', 'created': 100, 'expired': 300,
         'data': 'bar'},
        {'id': 'gap', 'from': 'gap', 'to': 'same1',
         '_key': 'gap_v2', '_id': 'def_e/gap_v2', '_from': 'v/gap_v2', '_to': 'v/same1_v0',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'bar'},
        {'id': 'old', 'from': 'old', 'to': 'up1',
         '_key': 'old_v0', '_id': 'def_e/old_v0', '_from': 'v/old_v0', '_to': 'v/up1_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'foo'},
        {'id': 'up1', 'from': 'same1', 'to': 'up1',
         '_key': 'up1_v0', '_id': 'def_e/up1_v0', '_from': 'v/same1_v0', '_to': 'v/up1_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'bar'},
        {'id': 'up1', 'from': 'same1', 'to': 'up1',
         '_key': 'up1_v2', '_id': 'def_e/up1_v2', '_from': 'v/same1_v0', '_to': 'v/up1_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'bar'},
    ]

    _check_docs(arango_db, def_e_expected, 'def_e')

    e1_expected = [
        {'id': 'old', 'from': 'old', 'to': 'same1',
         '_key': 'old_v0', '_id': 'e1/old_v0', '_from': 'v/old_v0', '_to': 'v/same1_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'baz'},
        {'id': 'same', 'from': 'same1', 'to': 'same2',
         '_key': 'same_v0', '_id': 'e1/same_v0', '_from': 'v/same1_v0', '_to': 'v/same2_v0',
         'first_version': 'v0', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'data': 'bing'},
    ]

    _check_docs(arango_db, e1_expected, 'e1')

    e2_expected = [
        {'id': 'change', 'from': 'same1', 'to': 'same2',
         '_key': 'change_v0', '_id': 'e2/change_v0', '_from': 'v/same1_v0', '_to': 'v/same2_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'baz'},
        {'id': 'change', 'from': 'same1', 'to': 'same2',
         '_key': 'change_v2', '_id': 'e2/change_v2', '_from': 'v/same1_v0', '_to': 'v/same2_v0',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'boo'},
        {'id': 'up2', 'from': 'up2', 'to': 'same2',
         '_key': 'up2_v0', '_id': 'e2/up2_v0', '_from': 'v/up2_v0', '_to': 'v/same2_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'boof'},
        {'id': 'up2', 'from': 'up2', 'to': 'same2',
         '_key': 'up2_v2', '_id': 'e2/up2_v2', '_from': 'v/up2_v2', '_to': 'v/same2_v0',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'boof'},
    ]

    _check_docs(arango_db, e2_expected, 'e2')

def test_merge_edges(arango_db):
    """
    Test that merge edges are handled appropriately.
    """

    vcol = arango_db.create_collection('v')
    ecol = arango_db.create_collection('e', edge=True)
    arango_db.create_collection('m', edge=True)
    
    _import_bulk(
        vcol,
        [
         {'id': 'root', 'data': 'foo'},   # will not change
         {'id': 'merged', 'data': 'bar'}, # will be merged
         {'id': 'target', 'data': 'baz'}, # will not change
        ],
        100, ADB_MAX_TIME, 'v1')
    
    _import_bulk(
        ecol,
        [
         {'id': 'to_m', 'from': 'root', 'to': 'merged', 'data': 'foo'}, # will be deleted
         {'id': 'to_t', 'from': 'root', 'to': 'target', 'data': 'bar'}  # shouldn't be touched
        ],
        100, ADB_MAX_TIME, 'v1', vert_col_name=vcol.name)

    vsource = [
        {'id': 'root', 'data': 'foo'},   # will not change
        {'id': 'target', 'data': 'baz'}, # will not change
    ]

    esource = [
        {'id': 'to_t', 'from': 'root', 'to': 'target', 'data': 'bar'} # no change
    ]

    msource = [
        {'id': 'f_to_t', 'from': 'fake1', 'to': 'target', 'data': 'whee'},  # will be ignored
        {'id': 'm_to_t', 'from': 'merged', 'to': 'target', 'data': 'woo'},  # will be applied
        {'id': 't_to_f', 'from': 'target', 'to': 'fake2', 'data': 'whoa'}   # will be ignored
    ]

    db = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e',
            merge_collection='m')
    
    load_graph_delta(vsource, esource, db, 500, 'v2', merge_source=msource)

    vexpected = [
        {'id': 'root', '_key': 'root_v1', '_id': 'v/root_v1',
         'first_version': 'v1', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'data': 'foo'},
        {'id': 'merged', '_key': 'merged_v1', '_id': 'v/merged_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'bar'},
        {'id': 'target', '_key': 'target_v1', '_id': 'v/target_v1',
         'first_version': 'v1', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'data': 'baz'},
    ]

    _check_docs(arango_db, vexpected, 'v')

    e_expected = [
        {'id': 'to_m', 'from': 'root', 'to': 'merged',
         '_key': 'to_m_v1', '_id': 'e/to_m_v1', '_from': 'v/root_v1', '_to': 'v/merged_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'data': 'foo'},
        {'id': 'to_t', 'from': 'root', 'to': 'target',
         '_key': 'to_t_v1', '_id': 'e/to_t_v1', '_from': 'v/root_v1', '_to': 'v/target_v1',
         'first_version': 'v1', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'data': 'bar'},
    ]

    _check_docs(arango_db, e_expected, 'e')

    m_expected = [
        {'id': 'm_to_t', 'from': 'merged', 'to': 'target',
         '_key': 'm_to_t_v2', '_id': 'm/m_to_t_v2', '_from': 'v/merged_v1', '_to': 'v/target_v1',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'data': 'woo'},
    ]

    _check_docs(arango_db, m_expected, 'm')

# modifies docs in place!
# vert_col_name != None implies and edge
def _import_bulk(
        col,
        docs,
        created,
        expired,
        first_version,
        last_version=None,
        vert_col_name=None):
    last_version = last_version if last_version else first_version
    for d in docs:
        d['_key'] = d['id'] + '_' + first_version
        if vert_col_name:
            d['_from'] = vert_col_name + '/' + d['from'] + '_' + first_version
            d['_to'] = vert_col_name + '/' + d['to'] + '_' + first_version
        d['created'] = created
        d['expired'] = expired
        d['first_version'] = first_version
        d['last_version'] = last_version
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