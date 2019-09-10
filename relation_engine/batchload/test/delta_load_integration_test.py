# Tests the delta load algorithm using an arangodb database.
# As such, the tests are fairly complex.

# This does not test the database wrapper code - it has its own tests.

# TODO TEST start a new arango instance as part of the tests so:
# a) we remove chance of data corruption and 
# b) we don't leave test data around

# TODO TEST add unit tests for the delta load algorithm with a db mock.

from relation_engine.batchload.time_travelling_database import ArangoBatchTimeTravellingDB
from relation_engine.batchload.time_travelling_database import ArangoBatchTimeTravellingDBFactory
from relation_engine.batchload.delta_load import load_graph_delta, roll_back_last_load
from relation_engine.batchload.test.test_helpers import create_timetravel_collection
from relation_engine.batchload.test.test_helpers import check_docs, check_exception
from arango import ArangoClient
import datetime
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

##########################################
# Delta load tests
##########################################

def test_merge_setup_fail(arango_db):
    """
    Tests that the algorithm fails to start if a merge source is specified but a merge collection
    is not
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('r')

    att = ArangoBatchTimeTravellingDB(arango_db, 'r', 'v', default_edge_collection='e')

    # sources are fake, but real not necessary to trigger error
    check_exception(lambda: load_graph_delta('ns', [], [], att, 1, 1, "2", merge_source=[{}]),
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
    vcol = create_timetravel_collection(arango_db, 'v')
    def_ecol = create_timetravel_collection(arango_db, 'def_e', edge=True)
    e1col = create_timetravel_collection(arango_db, 'e1', edge=True)
    e2col = create_timetravel_collection(arango_db, 'e2', edge=True)
    arango_db.create_collection('r')

    _import_bulk(
        vcol,
        [
         {'id': 'expire', 'data': 'foo'},     # expired nodes shouldn't be touched
         {'id': 'gap', 'data': 'super sweet'}, # even if reintroduced later
        ],
        100, 300, 99, 299, 'v0')

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
        100, ADB_MAX_TIME, 99, ADB_MAX_TIME, 'v0', 'v1')

    _import_bulk(
        def_ecol,
        [
         {'id': 'expire', 'from': 'expire', 'to': 'same2', 'data': 'foo'},  # shouldn't be touched
         {'id': 'gap', 'from': 'gap', 'to': 'same1', 'data': 'bar'}         # ditto
        ],
        100, 300, 99, 299, 'v0', vert_col_name=vcol.name)

    _import_bulk(
        def_ecol,
        [
         {'id': 'old', 'from': 'old', 'to': 'up1', 'data': 'foo'},  # will be deleted
         {'id': 'up1', 'from': 'same1', 'to': 'up1', 'data': 'bar'} # will be updated to new up1
        ],
        100, ADB_MAX_TIME, 99, ADB_MAX_TIME, 'v0', 'v1', vert_col_name=vcol.name)

    _import_bulk(
        e1col,
        [
         {'id': 'old', 'from': 'old', 'to': 'same1', 'data': 'baz'},    # will be deleted
         {'id': 'same', 'from': 'same1', 'to': 'same2', 'data': 'bing'} # no change
        ],
        100, ADB_MAX_TIME, 99, ADB_MAX_TIME, 'v0', 'v1', vert_col_name=vcol.name)

    _import_bulk(
        e2col,
        [
         {'id': 'change', 'from': 'same1', 'to': 'same2', 'data': 'baz'}, # will be updated
         {'id': 'up2', 'from': 'up2', 'to': 'same2', 'data': 'boof'}      # will be updated to up2
        ],
        100, ADB_MAX_TIME, 99, ADB_MAX_TIME, 'v0', 'v1', vert_col_name=vcol.name)

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

    db = ArangoBatchTimeTravellingDB(arango_db, 'r', 'v', default_edge_collection='def_e',
            edge_collections=['e1', 'e2'])
    
    if batchsize:
        load_graph_delta('ns', vsource, esource, db, 500, 400, 'v2', batch_size=batchsize)
    else: 
        load_graph_delta('ns', vsource, esource, db, 500, 400, 'v2')

    vexpected = [
        {'id': 'expire', '_key': 'expire_v0', '_id': 'v/expire_v0',
         'first_version': 'v0', 'last_version': 'v0', 'created': 100, 'expired': 300,
         'release_created': 99, 'release_expired': 299, 'data': 'foo'},
        {'id': 'gap', '_key': 'gap_v0', '_id': 'v/gap_v0',
         'first_version': 'v0', 'last_version': 'v0', 'created': 100, 'expired': 300,
         'release_created': 99, 'release_expired': 299, 'data': 'super sweet'},
        {'id': 'gap', '_key': 'gap_v2', '_id': 'v/gap_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'release_created': 400, 'release_expired': ADB_MAX_TIME, 'data': 'super sweet'},
        {'id': 'old', '_key': 'old_v0', '_id': 'v/old_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'foo'},
        {'id': 'same1', '_key': 'same1_v0', '_id': 'v/same1_v0',
         'first_version': 'v0', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': {'bar': 'baz'}},
        {'id': 'same2', '_key': 'same2_v0', '_id': 'v/same2_v0',
         'first_version': 'v0', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': ['bar', 'baz']},
        {'id': 'up1', '_key': 'up1_v0', '_id': 'v/up1_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': {'old': 'data'}},
        {'id': 'up1', '_key': 'up1_v2', '_id': 'v/up1_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'release_created': 400, 'release_expired': ADB_MAX_TIME, 'data': {'new': 'data'}},
        {'id': 'up2', '_key': 'up2_v0', '_id': 'v/up2_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': ['old', 'data']},
        {'id': 'up2', '_key': 'up2_v2', '_id': 'v/up2_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'release_created': 400, 'release_expired': ADB_MAX_TIME, 'data': ['old', 'data1']},
    ]

    check_docs(arango_db, vexpected, 'v')

    def_e_expected = [
        {'id': 'expire', 'from': 'expire', 'to': 'same2',
         '_key': 'expire_v0', '_id': 'def_e/expire_v0', '_from': 'v/expire_v0', '_to': 'v/same2_v0',
         'first_version': 'v0', 'last_version': 'v0', 'created': 100, 'expired': 300,
         'release_created': 99, 'release_expired': 299, 'data': 'foo'},
        {'id': 'gap', 'from': 'gap', 'to': 'same1',
         '_key': 'gap_v0', '_id': 'def_e/gap_v0', '_from': 'v/gap_v0', '_to': 'v/same1_v0',
         'first_version': 'v0', 'last_version': 'v0', 'created': 100, 'expired': 300,
         'release_created': 99, 'release_expired': 299, 'data': 'bar'},
        {'id': 'gap', 'from': 'gap', 'to': 'same1',
         '_key': 'gap_v2', '_id': 'def_e/gap_v2', '_from': 'v/gap_v2', '_to': 'v/same1_v0',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'release_created': 400, 'release_expired': ADB_MAX_TIME, 'data': 'bar'},
        {'id': 'old', 'from': 'old', 'to': 'up1',
         '_key': 'old_v0', '_id': 'def_e/old_v0', '_from': 'v/old_v0', '_to': 'v/up1_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'foo'},
        {'id': 'up1', 'from': 'same1', 'to': 'up1',
         '_key': 'up1_v0', '_id': 'def_e/up1_v0', '_from': 'v/same1_v0', '_to': 'v/up1_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'bar'},
        {'id': 'up1', 'from': 'same1', 'to': 'up1',
         '_key': 'up1_v2', '_id': 'def_e/up1_v2', '_from': 'v/same1_v0', '_to': 'v/up1_v2',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'release_created': 400, 'release_expired': ADB_MAX_TIME, 'data': 'bar'},
    ]

    check_docs(arango_db, def_e_expected, 'def_e')

    e1_expected = [
        {'id': 'old', 'from': 'old', 'to': 'same1',
         '_key': 'old_v0', '_id': 'e1/old_v0', '_from': 'v/old_v0', '_to': 'v/same1_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'baz'},
        {'id': 'same', 'from': 'same1', 'to': 'same2',
         '_key': 'same_v0', '_id': 'e1/same_v0', '_from': 'v/same1_v0', '_to': 'v/same2_v0',
         'first_version': 'v0', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'bing'},
    ]

    check_docs(arango_db, e1_expected, 'e1')

    e2_expected = [
        {'id': 'change', 'from': 'same1', 'to': 'same2',
         '_key': 'change_v0', '_id': 'e2/change_v0', '_from': 'v/same1_v0', '_to': 'v/same2_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'baz'},
        {'id': 'change', 'from': 'same1', 'to': 'same2',
         '_key': 'change_v2', '_id': 'e2/change_v2', '_from': 'v/same1_v0', '_to': 'v/same2_v0',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'release_created': 400, 'release_expired': ADB_MAX_TIME, 'data': 'boo'},
        {'id': 'up2', 'from': 'up2', 'to': 'same2',
         '_key': 'up2_v0', '_id': 'e2/up2_v0', '_from': 'v/up2_v0', '_to': 'v/same2_v0',
         'first_version': 'v0', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'boof'},
        {'id': 'up2', 'from': 'up2', 'to': 'same2',
         '_key': 'up2_v2', '_id': 'e2/up2_v2', '_from': 'v/up2_v2', '_to': 'v/same2_v0',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'release_created': 400, 'release_expired': ADB_MAX_TIME, 'data': 'boof'},
    ]

    check_docs(arango_db, e2_expected, 'e2')

    registry_expected = {
        '_key': 'ns_v2',
        '_id': 'r/ns_v2',
        'load_namespace': 'ns',
        'load_version': 'v2',
        'load_timestamp': 500,
        'release_timestamp': 400,
        # 'start_time': 0,
        # 'completion_time': 0,
        'state': 'complete',
        'vertex_collection': 'v',
        'merge_collection': None, 
        'edge_collections': ['def_e', 'e1', 'e2']
    }

    _check_registry_doc(arango_db, registry_expected, 'r', compare_times_to_now=True)


def test_merge_edges(arango_db):
    """
    Test that merge edges are handled appropriately.
    """

    vcol = create_timetravel_collection(arango_db, 'v')
    ecol = create_timetravel_collection(arango_db, 'e', edge=True)
    create_timetravel_collection(arango_db, 'm', edge=True)
    arango_db.create_collection('r')
    
    _import_bulk(
        vcol,
        [
         {'id': 'root', 'data': 'foo'},   # will not change
         {'id': 'merged', 'data': 'bar'}, # will be merged
         {'id': 'target', 'data': 'baz'}, # will not change
        ],
        100, ADB_MAX_TIME, 99, ADB_MAX_TIME, 'v1')
    
    _import_bulk(
        ecol,
        [
         {'id': 'to_m', 'from': 'root', 'to': 'merged', 'data': 'foo'}, # will be deleted
         {'id': 'to_t', 'from': 'root', 'to': 'target', 'data': 'bar'}  # shouldn't be touched
        ],
        100, ADB_MAX_TIME, 99, ADB_MAX_TIME, 'v1', vert_col_name=vcol.name)

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

    db = ArangoBatchTimeTravellingDB(arango_db, 'r', 'v', default_edge_collection='e',
            merge_collection='m')
    
    load_graph_delta('mns', vsource, esource, db, 500, 400, 'v2', merge_source=msource)

    vexpected = [
        {'id': 'root', '_key': 'root_v1', '_id': 'v/root_v1',
         'first_version': 'v1', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'foo'},
        {'id': 'merged', '_key': 'merged_v1', '_id': 'v/merged_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'bar'},
        {'id': 'target', '_key': 'target_v1', '_id': 'v/target_v1',
         'first_version': 'v1', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'baz'},
    ]

    check_docs(arango_db, vexpected, 'v')

    e_expected = [
        {'id': 'to_m', 'from': 'root', 'to': 'merged',
         '_key': 'to_m_v1', '_id': 'e/to_m_v1', '_from': 'v/root_v1', '_to': 'v/merged_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 100, 'expired': 499,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'foo'},
        {'id': 'to_t', 'from': 'root', 'to': 'target',
         '_key': 'to_t_v1', '_id': 'e/to_t_v1', '_from': 'v/root_v1', '_to': 'v/target_v1',
         'first_version': 'v1', 'last_version': 'v2', 'created': 100, 'expired': ADB_MAX_TIME,
         'release_created': 99, 'release_expired': ADB_MAX_TIME, 'data': 'bar'},
    ]

    check_docs(arango_db, e_expected, 'e')

    m_expected = [
        {'id': 'm_to_t', 'from': 'merged', 'to': 'target',
         '_key': 'm_to_t_v2', '_id': 'm/m_to_t_v2', '_from': 'v/merged_v1', '_to': 'v/target_v1',
         'first_version': 'v2', 'last_version': 'v2', 'created': 500, 'expired': ADB_MAX_TIME,
         'release_created': 400, 'release_expired': ADB_MAX_TIME, 'data': 'woo'},
    ]

    check_docs(arango_db, m_expected, 'm')

    registry_expected = {
        '_key': 'mns_v2',
        '_id': 'r/mns_v2',
        'load_namespace': 'mns',
        'load_version': 'v2',
        'load_timestamp': 500,
        'release_timestamp': 400,
        # 'start_time': 0,
        # 'completion_time': 0,
        'state': 'complete',
        'vertex_collection': 'v',
        'merge_collection': 'm', 
        'edge_collections': ['e']
    }

    _check_registry_doc(arango_db, registry_expected, 'r', compare_times_to_now=True)

######################################
# Rollback tests
######################################

def test_rollback_fail_nothing_to_roll_back(arango_db):
    """
    Test that a rollback fails if theres < 2 loads registered.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('r')

    db = ArangoBatchTimeTravellingDB(arango_db, 'r', 'v', default_edge_collection='e')
    
    db.register_load_start('ns1', 'v1', 1000, 500, 100)
    db.register_load_complete('ns1', 'v1', 150)

    check_exception(lambda: roll_back_last_load(db, 'ns1'), ValueError,
        'Nothing to roll back')

def test_rollback_with_merge_collection(arango_db):
    """
    Test rolling back a load including a merge collection.
    """
    vcol = create_timetravel_collection(arango_db, 'v')
    edcol = create_timetravel_collection(arango_db, 'def_e', edge=True)
    e1col = create_timetravel_collection(arango_db, 'e1', edge=True)
    e2col = create_timetravel_collection(arango_db, 'e2', edge=True)
    mcol = create_timetravel_collection(arango_db, 'm', edge=True)
    arango_db.create_collection('r')

    m = ADB_MAX_TIME

    _import_v(vcol, {'id': '1', 'k': '1'}, 0, m, 0, m, 'v1', 'v2')
    _import_v(vcol, {'id': '2', 'k': '2'}, 300, m, 299, m, 'v2', 'v2')
    _import_v(vcol, {'id': '3', 'k': '3'}, 0, 299, 0, 298, 'v1', 'v1')
    _import_v(vcol, {'id': '3', 'k': '3'}, 300, m, 299, m, 'v2', 'v2')
    _import_v(vcol, {'id': '4', 'k': '4'}, 0, 299, 0, 298, 'v1', 'v1')

    _import_e(edcol, {'id': '1', 'to': '1', 'from': '1', 'k': '1'}, 0, m, 0, m, 'v1', 'v2', 'f')
    _import_e(edcol, {'id': '2', 'to': '2', 'from': '2', 'k': '2'}, 300, m, 299, m,'v2', 'v2', 'f')

    _import_e(e1col, {'id': '1', 'to': '1', 'from': '1', 'k': '1'},
        0, 299, 0, 298, 'v1', 'v1', 'f')
    _import_e(e1col, {'id': '1', 'to': '1', 'from': '1', 'k': '1'},
        300, m, 299, m, 'v2', 'v2', 'f')

    _import_e(e2col, {'id': '1', 'to': '1', 'from': '1', 'k': '1'},
        0, 299, 0, 298, 'v1', 'v1', 'f')

    # merge edges are never updated once created
    _import_e(mcol, {'id': '1', 'to': '1', 'from': '1', 'k': '1'}, 0, m, 0, m, 'v1', 'v1', 'f')
    _import_e(mcol, {'id': '2', 'to': '2', 'from': '2', 'k': '2'}, 300, m, 299, m, 'v2', 'v2', 'f')

    db = ArangoBatchTimeTravellingDB(arango_db, 'r', 'v', default_edge_collection='def_e',
        edge_collections=['e1', 'e2'], merge_collection='m')

    db.register_load_start('ns1', 'v1', 0, 0, 4567)
    db.register_load_complete('ns1', 'v1', 5678)
    db.register_load_start('ns1', 'v2', 300, 250, 6789)
    db.register_load_complete('ns1', 'v2', 7890)

    fac = ArangoBatchTimeTravellingDBFactory(arango_db, 'r')

    roll_back_last_load(fac, 'ns1')

    vexpected = [
        {'id': '1', '_key': '1_v1', '_id': 'v/1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': ADB_MAX_TIME, 'k': '1'},
        {'id': '3', '_key': '3_v1', '_id': 'v/3_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': 298, 'k': '3'},
        {'id': '4', '_key': '4_v1', '_id': 'v/4_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': 298, 'k': '4'},
    ]

    check_docs(arango_db, vexpected, 'v')

    ed_expected = [
        {'id': '1', 'from': '1', 'to': '1',
         '_key': '1_v1', '_id': 'def_e/1_v1', '_from': 'f/1_v1', '_to': 'f/1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': ADB_MAX_TIME, 'k': '1'},
    ]

    check_docs(arango_db, ed_expected, 'def_e')

    e1_expected = [
        {'id': '1', 'from': '1', 'to': '1',
         '_key': '1_v1', '_id': 'e1/1_v1', '_from': 'f/1_v1', '_to': 'f/1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': 298, 'k': '1'},
    ]

    check_docs(arango_db, e1_expected, 'e1')

    e2_expected = [
        {'id': '1', 'from': '1', 'to': '1',
         '_key': '1_v1', '_id': 'e2/1_v1', '_from': 'f/1_v1', '_to': 'f/1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': 298, 'k': '1'},
    ]

    check_docs(arango_db, e2_expected, 'e2')

    m_expected = [
        {'id': '1', 'from': '1', 'to': '1',
         '_key': '1_v1', '_id': 'm/1_v1', '_from': 'f/1_v1', '_to': 'f/1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': ADB_MAX_TIME, 'k': '1'},
    ]

    check_docs(arango_db, m_expected, 'm')

    registry_expected = {
        '_key': 'ns1_v1',
        '_id': 'r/ns1_v1',
        'load_namespace': 'ns1',
        'load_version': 'v1',
        'load_timestamp': 0,
        'release_timestamp': 0,
        'start_time': 4567,
        'completion_time': 5678,
        'state': 'complete',
        'vertex_collection': 'v',
        'merge_collection': 'm', 
        'edge_collections': ['def_e', 'e1', 'e2']
    }

    _check_registry_doc(arango_db, registry_expected, 'r')

# trying to combine with above got too messy
def test_rollback_without_merge_collection(arango_db):
    """
    Test rolling back a load with no merge collection and only one edge collection.
    """
    vcol = create_timetravel_collection(arango_db, 'v')
    ecol = create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('r')

    m = ADB_MAX_TIME

    _import_v(vcol, {'id': '1', 'k': '1'}, 0, m, 0, m, 'v1', 'v2')
    _import_v(vcol, {'id': '2', 'k': '2'}, 300, m, 299, m, 'v2', 'v2')
    _import_v(vcol, {'id': '3', 'k': '3'}, 0, 299, 0, 298, 'v1', 'v1')
    _import_v(vcol, {'id': '3', 'k': '3'}, 300, m, 299, m, 'v2', 'v2')
    _import_v(vcol, {'id': '4', 'k': '4'}, 0, 299, 0, 298, 'v1', 'v1')

    _import_e(ecol, {'id': '1', 'to': '1', 'from': '1', 'k': '1'}, 0, m, 0, m, 'v1', 'v2', 'f')
    _import_e(ecol, {'id': '2', 'to': '2', 'from': '2', 'k': '2'}, 300, m, 299, m, 'v2', 'v2', 'f')
    _import_e(ecol, {'id': '3', 'to': '3', 'from': '3', 'k': '3'}, 0, 299, 0, 298, 'v1', 'v1', 'f')
    _import_e(ecol, {'id': '3', 'to': '3', 'from': '3', 'k': '3'}, 300, m, 399, 0, 'v2', 'v2', 'f')
    _import_e(ecol, {'id': '4', 'to': '4', 'from': '4', 'k': '4'}, 0, 299, 0, 298, 'v1', 'v1', 'f')

    db = ArangoBatchTimeTravellingDB(arango_db, 'r', 'v', default_edge_collection='e')

    db.register_load_start('ns1', 'v1', 0, 0, 4567)
    db.register_load_complete('ns1', 'v1', 5678)
    db.register_load_start('ns1', 'v2', 300, 250, 6789)
    db.register_load_complete('ns1', 'v2', 7890)

    fac = ArangoBatchTimeTravellingDBFactory(arango_db, 'r')

    roll_back_last_load(fac, 'ns1')

    vexpected = [
        {'id': '1', '_key': '1_v1', '_id': 'v/1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': ADB_MAX_TIME, 'k': '1'},
        {'id': '3', '_key': '3_v1', '_id': 'v/3_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': 298, 'k': '3'},
        {'id': '4', '_key': '4_v1', '_id': 'v/4_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': 298, 'k': '4'},
    ]

    check_docs(arango_db, vexpected, 'v')

    e_expected = [
        {'id': '1', 'from': '1', 'to': '1',
         '_key': '1_v1', '_id': 'e/1_v1', '_from': 'f/1_v1', '_to': 'f/1_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': ADB_MAX_TIME, 'k': '1'},
        {'id': '3', 'from': '3', 'to': '3',
         '_key': '3_v1', '_id': 'e/3_v1', '_from': 'f/3_v1', '_to': 'f/3_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': 298, 'k': '3'},
        {'id': '4', 'from': '4', 'to': '4',
         '_key': '4_v1', '_id': 'e/4_v1', '_from': 'f/4_v1', '_to': 'f/4_v1',
         'first_version': 'v1', 'last_version': 'v1', 'created': 0, 'expired': ADB_MAX_TIME,
         'release_created': 0, 'release_expired': 298, 'k': '4'},
    ]

    check_docs(arango_db, e_expected, 'e')

    registry_expected = {
        '_key': 'ns1_v1',
        '_id': 'r/ns1_v1',
        'load_namespace': 'ns1',
        'load_version': 'v1',
        'load_timestamp': 0,
        'release_timestamp': 0,
        'start_time': 4567,
        'completion_time': 5678,
        'state': 'complete',
        'vertex_collection': 'v',
        'merge_collection': None, 
        'edge_collections': ['e']
    }

    _check_registry_doc(arango_db, registry_expected, 'r')

######################################
# Helper funcs
######################################

# modifies docs in place!
# vert_col_name != None implies an edge
def _import_bulk(
        col,
        docs,
        created,
        expired,
        release_created,
        release_expired,
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
        d['release_created'] = release_created
        d['release_expired'] = release_expired
        d['first_version'] = first_version
        d['last_version'] = last_version
    col.import_bulk(docs)

# data will be modified in place
def _import_v(
        col,
        data,
        created,
        expired,
        release_created,
        release_expired,
        first_version,
        last_version):
    d = data
    d['_key'] = d['id'] + '_' + first_version
    d['created'] = created
    d['expired'] = expired
    d['release_created'] = release_created
    d['release_expired'] = release_expired
    d['first_version'] = first_version
    d['last_version'] = last_version
    col.import_bulk([d])

# data will be modified in place
def _import_e(
        col,
        data,
        created,
        expired,
        release_created,
        release_expired,
        first_version,
        last_version,
        vert_col_name):
    d = data
    d['_key'] = d['id'] + '_' + first_version
    d['_from'] = vert_col_name + '/' + d['from'] + '_' + first_version
    d['_to'] = vert_col_name + '/' + d['to'] + '_' + first_version
    d['created'] = created
    d['expired'] = expired
    d['release_created'] = release_created
    d['release_expired'] = release_expired
    d['first_version'] = first_version
    d['last_version'] = last_version
    col.import_bulk([d])


def _check_registry_doc(arango_db, expected, collection, compare_times_to_now=False):
    col = arango_db.collection(collection)
    assert col.count() == 1, 'Incorrect # of docs in registry collection ' + collection
    doc = col.get(expected['_key'])
    del doc['_rev']
    if compare_times_to_now:
        start = doc['start_time']
        del doc['start_time']
        end = doc['completion_time']
        del doc['completion_time']
        _assert_close_to_now_in_epoch_ms(start)
        _assert_close_to_now_in_epoch_ms(end)
    
    assert expected == doc

def _assert_close_to_now_in_epoch_ms(time):
    now = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000)
    assert now - 2000 < time
    assert now + 2000 > time   