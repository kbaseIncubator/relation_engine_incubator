# TODO TEST start a new arango instance as part of the tests so:
# a) we remove chance of data corruption and 
# b) we don't leave test data around

# TODO TEST more tests

from relation_engine.batchload.time_travelling_database import ArangoBatchTimeTravellingDB
from relation_engine.batchload.time_travelling_database import ArangoBatchTimeTravellingDBFactory
from relation_engine.batchload.test.test_helpers import create_timetravel_collection
from relation_engine.batchload.test.test_helpers import check_docs, check_exception
from arango import ArangoClient
from pytest import fixture

HOST = 'localhost'
PORT = 8529
DB_NAME = 'test_timetravel_delta_batch_load_db'

@fixture
def arango_db():
    client = ArangoClient(protocol='http', host=HOST, port=PORT)
    sys = client.db('_system', 'root', '', verify=True)
    sys.delete_database(DB_NAME, ignore_missing=True)
    sys.create_database(DB_NAME)
    db = client.db(DB_NAME)

    yield db

    sys.delete_database(DB_NAME)

def test_get_registry_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    assert att.get_registry_collection() == 'reg'

def test_get_vertex_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    assert att.get_vertex_collection() == 'v'

def test_get_default_edge_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])
    assert att.get_default_edge_collection() is None

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    assert att.get_default_edge_collection() is 'e'

def test_get_edge_collections(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e1', edge=True)
    create_timetravel_collection(arango_db, 'e2', edge=True)
    create_timetravel_collection(arango_db, 'e3', edge=True)
    arango_db.create_collection('reg')

    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e1')
    assert adbtt.get_edge_collections() == ['e1']

    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e2', 'e2', 'e1'])
    assert adbtt.get_edge_collections() == ['e1', 'e2']

    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e3',
        edge_collections=['e2', 'e2', 'e1', 'e3'])
    assert adbtt.get_edge_collections() == ['e1', 'e2', 'e3']

def test_get_merge_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    create_timetravel_collection(arango_db, 'm', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    assert att.get_merge_collection() is None

    att = ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', default_edge_collection='e', merge_collection='m')
    assert att.get_merge_collection() is 'm'

def test_init_fail_no_edge_collections(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'm', edge=True)
    arango_db.create_collection('reg')

    check_exception(lambda: ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', merge_collection='m'),
        ValueError, 'At least one edge collection must be specified')

def test_init_fail_bad_registry_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg', edge=True)

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e'),
        ValueError, 'reg is not a vertex collection')

def test_init_fail_bad_vertex_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(arango_db, 'e', 'reg', default_edge_collection='e'),
        ValueError, 'e is not a vertex collection')

def test_init_fail_bad_edge_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    col_name = 'edge'
    create_timetravel_collection(arango_db, col_name)
    arango_db.create_collection('reg')

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(
            arango_db, 'reg', 'v', default_edge_collection=col_name),
        ValueError, 'edge is not an edge collection')

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=[col_name]),
        ValueError, 'edge is not an edge collection')

def test_init_fail_bad_merge_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    create_timetravel_collection(arango_db, 'm')
    arango_db.create_collection('reg')

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(
            arango_db, 'reg', 'v', default_edge_collection='e', merge_collection='m'),
        ValueError, 'm is not an edge collection')

IDX_SPEC_ID = ("{'type': 'persistent', 'fields': ['id', 'expired', 'created'], " +
    "'sparse': False, 'unique': False}")

IDX_SPEC_EXP = ("{'type': 'persistent', 'fields': ['expired', 'created', 'last_version'], " +
    "'sparse': False, 'unique': False}")

def test_init_fail_no_id_index_vertex_collection(arango_db):
    col = arango_db.create_collection('v')
    col.add_persistent_index(['expired', 'created', 'last_version'])
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(
            arango_db, 'reg', 'v', default_edge_collection='e'),
        ValueError, f'Collection v is missing required index with specification {IDX_SPEC_ID}')

def test_init_fail_no_expire_index_vertex_collection(arango_db):
    col = arango_db.create_collection('v')
    col.add_persistent_index(['id', 'expired', 'created'])
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(
            arango_db, 'reg', 'v', default_edge_collection='e'),
        ValueError, f'Collection v is missing required index with specification {IDX_SPEC_EXP}')

def test_init_fail_no_id_index_edge_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    col = arango_db.create_collection('e', edge=True)
    col.add_persistent_index(['expired', 'created', 'last_version'])
    arango_db.create_collection('reg')

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(
            arango_db, 'reg', 'v', default_edge_collection='e'),
        ValueError, f'Collection e is missing required index with specification {IDX_SPEC_ID}')

def test_init_fail_no_expire_index_edge_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    col = arango_db.create_collection('e', edge=True)
    col.add_persistent_index(['id', 'expired', 'created'])
    arango_db.create_collection('reg')

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(
            arango_db, 'reg', 'v', default_edge_collection='e'),
        ValueError, f'Collection e is missing required index with specification {IDX_SPEC_EXP}')

def test_init_fail_no_id_index_merge_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    col = arango_db.create_collection('m', edge=True)
    col.add_persistent_index(['expired', 'created', 'last_version'])
    arango_db.create_collection('reg')

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(
            arango_db, 'reg', 'v', default_edge_collection='e', merge_collection='m'),
        ValueError, f'Collection m is missing required index with specification {IDX_SPEC_ID}')

def test_init_fail_no_expire_index_merge_collection(arango_db):
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    col = arango_db.create_collection('m', edge=True)
    col.add_persistent_index(['id', 'expired', 'created'])
    arango_db.create_collection('reg')

    check_exception(
        lambda: ArangoBatchTimeTravellingDB(
            arango_db, 'reg', 'v', default_edge_collection='e', merge_collection='m'),
        ValueError, f'Collection m is missing required index with specification {IDX_SPEC_EXP}')

def test_fail_no_default_edge_collection(arango_db):
    """
    Really should test this for all methods but that seems like a lot of tests for the same
    chunk of factored out code
    """
    create_timetravel_collection(arango_db, 'v')
    col_name = 'edge'
    create_timetravel_collection(arango_db, col_name, edge=True)
    arango_db.create_collection('reg')

    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=[col_name])

    check_exception(lambda:  adbtt.get_edges(['id'], 3), ValueError,
        'No default edge collection specified, must specify edge collection')

def test_fail_no_such_edge_collection(arango_db):
    """
    Really should test this for all methods but that seems like a lot of tests for the same
    chunk of factored out code

    Also tests that duplicate collection names don't cause a problem
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e1', edge=True)
    create_timetravel_collection(arango_db, 'e2', edge=True)
    create_timetravel_collection(arango_db, 'e3', edge=True)
    create_timetravel_collection(arango_db, 'm', edge=True)
    arango_db.create_collection('reg')

    # without merge collection
    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e1',
        edge_collections=['e2', 'e3', 'e2', 'e1'])

    check_exception(lambda:  adbtt.get_edges(['id1'], 300, edge_collection='e4'), ValueError,
        'Edge collection e4 was not registered at initialization')

    # with merge collection
    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e1',
        edge_collections=['e2', 'e3', 'e2', 'e1'], merge_collection='m')

    check_exception(lambda:  adbtt.get_edges(['id1'], 300, edge_collection='e4'), ValueError,
        'Edge collection e4 was not registered at initialization')

def test_register_load_start(arango_db):
    """
    Tests registering the start of a load with the db.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    att.register_load_start('GeneOntology', '09-08-07', 1000, 800, 500)

    expected = [{
        '_key': 'GeneOntology_09-08-07',
        '_id': 'reg/GeneOntology_09-08-07',
        'load_namespace': 'GeneOntology',
        'load_version': '09-08-07',
        'load_timestamp': 1000,
        'release_timestamp': 800,
        'start_time': 500,
        'completion_time': None,
        'state': 'in_progress',
        'vertex_collection': 'v',
        'merge_collection': None, 
        'edge_collections': ['e']
    }]

    check_docs(arango_db, expected, 'reg')

def test_register_load_start_with_merge_col_and_multiple_edge_cols(arango_db):
    """
    Tests registering the start of a load with the db with more collections.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'm', edge=True)
    create_timetravel_collection(arango_db, 'e1', edge=True)
    create_timetravel_collection(arango_db, 'e2', edge=True)
    create_timetravel_collection(arango_db, 'e3', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', merge_collection='m', default_edge_collection='e2',
        edge_collections=['e1', 'e3'])

    att.register_load_start('GeneOntology', '09-08-07', 1000, 800, 500)

    expected = [{
        '_key': 'GeneOntology_09-08-07',
        '_id': 'reg/GeneOntology_09-08-07',
        'load_namespace': 'GeneOntology',
        'load_version': '09-08-07',
        'load_timestamp': 1000,
        'release_timestamp': 800,
        'start_time': 500,
        'completion_time': None,
        'state': 'in_progress',
        'vertex_collection': 'v',
        'merge_collection': 'm', 
        'edge_collections': ['e1', 'e2', 'e3']
    }]

    check_docs(arango_db, expected, 'reg')

def test_register_load_start_fail_doc_exists(arango_db):
    """
    Test the case where a load is already in progress or already loaded and so this load should
    fail.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    att.register_load_start('GeneOntology', '09-08-07', 1000, 800, 500)
    
    check_exception(lambda: att.register_load_start('GeneOntology', '09-08-07', 8000, 7000, 400),
        ValueError, 'Load is already registered')

def test_register_load_complete(arango_db):
    """
    Tests registering the completion of a load with the db.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    att.register_load_start('GeneOntology', '09-08-07', 1000, 700, 500)
    att.register_load_complete('GeneOntology', '09-08-07', 800)

    expected = [{
        '_key': 'GeneOntology_09-08-07',
        '_id': 'reg/GeneOntology_09-08-07',
        'load_namespace': 'GeneOntology',
        'load_version': '09-08-07',
        'load_timestamp': 1000,
        'release_timestamp': 700,
        'start_time': 500,
        'completion_time': 800,
        'state': 'complete',
        'vertex_collection': 'v',
        'merge_collection': None, 
        'edge_collections': ['e']
    }]

    check_docs(arango_db, expected, 'reg')

def test_register_load_complete_fail_not_started(arango_db):
    """
    Test the case where a load is not registered and so cannot be completed.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    check_exception(lambda: att.register_load_complete('GeneOntology', '09-08-07', 800),
        ValueError, 'Load is not registered, cannot be completed')

def test_register_load_rollback(arango_db):
    """
    Tests registering the rollback of a load with the db.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    att.register_load_start('GeneOntology', '09-08-07', 1000, 700, 500)
    att.register_load_complete('GeneOntology', '09-08-07', 800)
    att.register_load_rollback('GeneOntology', '09-08-07')

    expected = [{
        '_key': 'GeneOntology_09-08-07',
        '_id': 'reg/GeneOntology_09-08-07',
        'load_namespace': 'GeneOntology',
        'load_version': '09-08-07',
        'load_timestamp': 1000,
        'release_timestamp': 700,
        'start_time': 500,
        'completion_time': 800,
        'state': 'rollback',
        'vertex_collection': 'v',
        'merge_collection': None, 
        'edge_collections': ['e']
    }]

    check_docs(arango_db, expected, 'reg')

def test_register_load_rollback_fail_not_started(arango_db):
    """
    Test the case where a load is not registered and so cannot be rolled back.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    check_exception(lambda: att.register_load_rollback('GeneOntology', '09-08-07'),
        ValueError, 'Load is not registered, cannot be rolled back')

def test_get_registered_loads_empty(arango_db):
    """
    Test getting the set of registered loads when there are none to get.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    att.register_load_start('ns2', 'v3', 700, 600, 400)

    assert att.get_registered_loads('ns1') == []

def test_get_registered_loads(arango_db):
    """
    Test getting the set of registered loads.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    att.register_load_start('ns1', 'v3', 700, 620, 400)
    att.register_load_start('ns1', 'v1', 500, 410, 300)
    att.register_load_complete('ns1', 'v1', 350)
    att.register_load_start('ns1', 'v2', 600, 510, 360)
    att.register_load_complete('ns1', 'v2', 390)
    att.register_load_start('ns2', 'v3', 700, 610, 400)

    expected = [
        {'_key': 'ns1_v3',
         '_id': 'reg/ns1_v3',
         'load_namespace': 'ns1',
         'load_version': 'v3',
         'load_timestamp': 700,
         'release_timestamp': 620,
         'start_time': 400,
         'completion_time': None,
         'state': 'in_progress',
         'vertex_collection': 'v',
         'merge_collection': None, 
         'edge_collections': ['e']
        },
        {'_key': 'ns1_v2',
         '_id': 'reg/ns1_v2',
         'load_namespace': 'ns1',
         'load_version': 'v2',
         'load_timestamp': 600,
         'release_timestamp': 510,
         'start_time': 360,
         'completion_time': 390,
         'state': 'complete',
         'vertex_collection': 'v',
         'merge_collection': None, 
         'edge_collections': ['e']
        },
        {'_key': 'ns1_v1',
         '_id': 'reg/ns1_v1',
         'load_namespace': 'ns1',
         'load_version': 'v1',
         'load_timestamp': 500,
         'release_timestamp': 410,
         'start_time': 300,
         'completion_time': 350,
         'state': 'complete',
         'vertex_collection': 'v',
         'merge_collection': None, 
         'edge_collections': ['e']
        },
    ]
    got = att.get_registered_loads('ns1')
    assert got == expected

    expected = [
        {'_key': 'ns2_v3',
         '_id': 'reg/ns2_v3',
         'load_namespace': 'ns2',
         'load_version': 'v3',
         'load_timestamp': 700,
         'release_timestamp': 610,
         'start_time': 400,
         'completion_time': None,
         'state': 'in_progress',
         'vertex_collection': 'v',
         'merge_collection': None, 
         'edge_collections': ['e']
        },
    ]
    got = att.get_registered_loads('ns2')
    assert got == expected

def test_delete_registered_load(arango_db):
    """
    Test deleting a registered load.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    att.register_load_start('ns1', 'v3', 700, 610, 400)
    att.register_load_start('ns1', 'v1', 500, 410, 300)
    att.register_load_complete('ns1', 'v1', 350)
    att.register_load_start('ns1', 'v2', 600, 510, 360)
    att.register_load_complete('ns1', 'v2', 390)
    att.delete_registered_load('ns1', 'v3')

    expected = [
        {'_key': 'ns1_v2',
         '_id': 'reg/ns1_v2',
         'load_namespace': 'ns1',
         'load_version': 'v2',
         'load_timestamp': 600,
         'release_timestamp': 510,
         'start_time': 360,
         'completion_time': 390,
         'state': 'complete',
         'vertex_collection': 'v',
         'merge_collection': None, 
         'edge_collections': ['e']
        },
        {'_key': 'ns1_v1',
         '_id': 'reg/ns1_v1',
         'load_namespace': 'ns1',
         'load_version': 'v1',
         'load_timestamp': 500,
         'release_timestamp': 410,
         'start_time': 300,
         'completion_time': 350,
         'state': 'complete',
         'vertex_collection': 'v',
         'merge_collection': None, 
         'edge_collections': ['e']
        },
    ]
    got = att.get_registered_loads('ns1')
    assert got == expected

def test_delete_registered_load_fail_no_load(arango_db):
    """
    Test failure of a registered load deletion because there's nothing to delete.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    att.register_load_start('ns1', 'v3', 700, 600, 400)
    att.register_load_start('ns1', 'v1', 500, 400, 300)
    att.register_load_complete('ns1', 'v1', 350)

    check_exception(lambda: att.delete_registered_load('ns1', 'v2'),
        ValueError, 'There is no load version v2 in namespace ns1')

def test_get_vertices(arango_db):
    """
    Tests that getting a vertex returns the correct vertex. In particular checks for OB1 errors.
    """
    col_name = 'verts'
    col = create_timetravel_collection(arango_db, col_name)
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    # it's assumed that given an id and a timestamp there's <= 1 match in the collection
    col.import_bulk([{'_key': '1', 'id': 'foo', 'created': 100, 'expired': 600},
                     {'_key': '2', 'id': 'bar', 'created': 100, 'expired': 200},
                     {'_key': '3', 'id': 'bar', 'created': 201, 'expired': 300},
                     {'_key': '4', 'id': 'bar', 'created': 301, 'expired': 400},
                     ])

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', col_name, edge_collections=['e'])

    ret = att.get_vertices(['bar'], 201)
    assert ret == {
        'bar': {'_key': '3', '_id': 'verts/3', 'id': 'bar', 'created': 201, 'expired': 300}
    }

    ret = att.get_vertices(['bar'], 300).get('bar')
    assert ret == {'_key': '3', '_id': 'verts/3', 'id': 'bar', 'created': 201, 'expired': 300}

    ret = att.get_vertices(['foo'], 250).get('foo')
    assert ret == {'_key': '1', '_id': 'verts/1', 'id': 'foo', 'created': 100, 'expired': 600}

    ret = att.get_vertices(['bar', 'foo'], 250)
    assert ret == {
        'bar': {'_key': '3', '_id': 'verts/3', 'id': 'bar', 'created': 201, 'expired': 300},
        'foo': {'_key': '1', '_id': 'verts/1', 'id': 'foo', 'created': 100, 'expired': 600}
    }

    assert att.get_vertices(['bar'], 99) == {}
    assert att.get_vertices(['bar'], 401) == {}

    col.insert({'_key': '5', 'id': 'bar', 'created': 150, 'expired': 250})

    check_exception(lambda:  att.get_vertices(['bar'], 200), ValueError,
        'db contains > 1 document for id bar, timestamp 200, collection verts')

def test_get_edges(arango_db):
    """
    Tests that getting a edge returns the correct edge. In particular checks for OB1 errors.
    """

    create_timetravel_collection(arango_db, 'v')
    col_name = 'edges'
    col = create_timetravel_collection(arango_db, col_name, edge=True)
    arango_db.create_collection('reg')

    # it's assumed that given an id and a timestamp there's <= 1 match in the collection
    col.import_bulk([{'_key': '1', '_from': 'fake/1', '_to': 'fake/2', 'id': 'foo',
                      'created': 100, 'expired': 600},
                     {'_key': '2', '_from': 'fake/1', '_to': 'fake/2', 'id': 'bar',
                      'created': 100, 'expired': 200},
                     {'_key': '3', '_from': 'fake/1', '_to': 'fake/2', 'id': 'bar',
                      'created': 201, 'expired': 300},
                     {'_key': '4', '_from': 'fake/1', '_to': 'fake/2', 'id': 'bar',
                      'created': 301, 'expired': 400},
                     ])

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection=col_name)

    ret = att.get_edges(['bar'], 201)
    assert ret == {'bar': {'_key': '3', '_id': 'edges/3', '_from': 'fake/1', '_to': 'fake/2',
                           'id': 'bar', 'created': 201, 'expired': 300}
    }

    ret = att.get_edges(['bar'], 300).get('bar')
    assert ret == {'_key': '3', '_id': 'edges/3', '_from': 'fake/1', '_to': 'fake/2', 'id': 'bar',
                   'created': 201, 'expired': 300}

    ret = att.get_edges(['foo'], 250).get('foo')
    assert ret == {'_key': '1', '_id': 'edges/1', '_from': 'fake/1', '_to': 'fake/2', 'id': 'foo',
                   'created': 100, 'expired': 600}

    ret = att.get_edges(['foo', 'bar'], 250)
    assert ret == {'foo': {'_key': '1', '_id': 'edges/1', '_from': 'fake/1', '_to': 'fake/2',
                           'id': 'foo', 'created': 100, 'expired': 600},
                   'bar': {'_key': '3', '_id': 'edges/3', '_from': 'fake/1', '_to': 'fake/2',
                           'id': 'bar', 'created': 201, 'expired': 300}
    }

    assert att.get_edges(['bar'], 99) == {}
    assert att.get_edges(['bar'], 401) == {}

    col.insert({'_key': '5', '_from': 'fake/1', '_to': 'fake/2', 'id': 'bar',
                'created': 150, 'expired': 250})

    check_exception(lambda:  att.get_edges(['bar'], 200), ValueError,
        'db contains > 1 document for id bar, timestamp 200, collection edges')

def test_expire_extant_vertices_without_last_version(arango_db):
    """
    Tests expiring vertices that exist at a specfic time without a given last version.
    """
    col_name = 'verts'
    col = create_timetravel_collection(arango_db, col_name)
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    test_data = [
        {'_key': '0', 'id': 'baz', 'created': 100, 'expired': 300, 'last_version': '2'},
        {'_key': '1', 'id': 'foo', 'created': 100, 'expired': 600, 'last_version': '1'},
        {'_key': '2', 'id': 'bar', 'created': 100, 'expired': 200, 'last_version': '1'},
        {'_key': '3', 'id': 'bar', 'created': 201, 'expired': 300, 'last_version': '2'},
        {'_key': '4', 'id': 'bar', 'created': 301, 'expired': 400, 'last_version': '2'},
        ]
    col.import_bulk(test_data)

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', col_name,    default_edge_collection='e')

    # test 1
    att.expire_extant_vertices_without_last_version(100, "2")

    expected = [
        {'_key': '0', '_id': 'verts/0', 'id': 'baz', 'created': 100, 'expired': 300,
         'last_version': '2'},
        {'_key': '1', '_id': 'verts/1', 'id': 'foo', 'created': 100, 'expired': 100,
         'last_version': '1'},
        {'_key': '2', '_id': 'verts/2', 'id': 'bar', 'created': 100, 'expired': 100,
         'last_version': '1'},
        {'_key': '3', '_id': 'verts/3', 'id': 'bar', 'created': 201, 'expired': 300,
         'last_version': '2'},
        {'_key': '4', '_id': 'verts/4', 'id': 'bar', 'created': 301, 'expired': 400,
         'last_version': '2'},
        ]

    check_docs(arango_db, expected, col_name)

    # test 2
    col.delete_match({})
    col.import_bulk(test_data)

    att.expire_extant_vertices_without_last_version(299, "1")

    expected = [
        {'_key': '0', '_id': 'verts/0', 'id': 'baz', 'created': 100, 'expired': 299,
         'last_version': '2'},
        {'_key': '1', '_id': 'verts/1', 'id': 'foo', 'created': 100, 'expired': 600,
         'last_version': '1'},
        {'_key': '2', '_id': 'verts/2', 'id': 'bar', 'created': 100, 'expired': 200,
         'last_version': '1'},
        {'_key': '3', '_id': 'verts/3', 'id': 'bar', 'created': 201, 'expired': 299,
         'last_version': '2'},
        {'_key': '4', '_id': 'verts/4', 'id': 'bar', 'created': 301, 'expired': 400,
         'last_version': '2'},
        ]

    check_docs(arango_db, expected, col_name)

def test_expire_extant_edges_without_last_version(arango_db):
    """
    Tests expiring egdes that exist at a specfic time without a given last version.

    Also test that a merge collection doesn't change the results without a default edge collection.
    """
    create_timetravel_collection(arango_db, 'v')
    col_name = 'edges'
    col = create_timetravel_collection(arango_db, col_name, edge=True)
    create_timetravel_collection(arango_db, 'm', edge=True)
    arango_db.create_collection('reg')

    test_data = [
        {'_key': '0', 'id': 'baz', 'created': 100, 'expired': 300, 'last_version': '2',
         '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '1', 'id': 'foo', 'created': 100, 'expired': 600, 'last_version': '1',
         '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '2', 'id': 'bar', 'created': 100, 'expired': 200, 'last_version': '1',
         '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '3', 'id': 'bar', 'created': 201, 'expired': 300, 'last_version': '2',
         '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '4', 'id': 'bar', 'created': 301, 'expired': 400, 'last_version': '2',
         '_from': 'fake/1', '_to': 'fake/2'},
        ]
    col.import_bulk(test_data)

    att = ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', edge_collections=[col_name], merge_collection='m')

    # test 1
    att.expire_extant_edges_without_last_version(100, '2', edge_collection=col_name)

    expected = [
        {'_key': '0', '_id': 'edges/0', 'id': 'baz', 'created': 100, 'expired': 300,
         'last_version': '2', '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '1', '_id': 'edges/1', 'id': 'foo', 'created': 100, 'expired': 100,
         'last_version': '1', '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '2', '_id': 'edges/2', 'id': 'bar', 'created': 100, 'expired': 100,
         'last_version': '1', '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '3', '_id': 'edges/3', 'id': 'bar', 'created': 201, 'expired': 300,
         'last_version': '2', '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '4', '_id': 'edges/4', 'id': 'bar', 'created': 301, 'expired': 400,
         'last_version': '2', '_from': 'fake/1', '_to': 'fake/2'},
        ]

    check_docs(arango_db, expected, col_name)

    # test 2
    col.delete_match({})
    col.import_bulk(test_data)

    att.expire_extant_edges_without_last_version(299, '1', edge_collection=col_name)

    expected = [
        {'_key': '0', '_id': 'edges/0', 'id': 'baz', 'created': 100, 'expired': 299,
         'last_version': '2', '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '1', '_id': 'edges/1', 'id': 'foo', 'created': 100, 'expired': 600,
         'last_version': '1', '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '2', '_id': 'edges/2', 'id': 'bar', 'created': 100, 'expired': 200,
         'last_version': '1', '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '3', '_id': 'edges/3', 'id': 'bar', 'created': 201, 'expired': 299,
         'last_version': '2', '_from': 'fake/1', '_to': 'fake/2'},
        {'_key': '4', '_id': 'edges/4', 'id': 'bar', 'created': 301, 'expired': 400,
         'last_version': '2', '_from': 'fake/1', '_to': 'fake/2'},
        ]

    check_docs(arango_db, expected, col_name)

##############################################
# Load reversion function tests
##############################################

REVERT_TEST_DATA = [
    {'_key': '0', 'id': 'baz', 'created': 100, 'expired': 300, 'last_version': '2'},
    {'_key': '1', 'id': 'foo', 'created': 100, 'expired': 600, 'last_version': '1'},
    {'_key': '2', 'id': 'bar', 'created': 100, 'expired': 200, 'last_version': '1'},
    {'_key': '3', 'id': 'bar', 'created': 201, 'expired': 300, 'last_version': '2'},
    {'_key': '4', 'id': 'bar', 'created': 301, 'expired': 400, 'last_version': '2'},
    ]

def _prep_data_for_revert_tests(colname, edge=False):
    actual_td = []
    actual_expected = []
    for e in REVERT_TEST_DATA:
        e = dict(e)
        e['_id'] = colname + '/' + e['_key']
        actual_expected.append(e)
    if edge: 
        for td in REVERT_TEST_DATA:
            td = dict(td)
            td['_from'] = 'garbage/2'
            td['_to'] = 'garbage/1'
            actual_td.append(td)
        for e in actual_expected:
            e['_from'] = 'garbage/2'
            e['_to'] = 'garbage/1'
    else:
        actual_td = REVERT_TEST_DATA

    return actual_td, actual_expected

def test_delete_created_documents_noop(arango_db):
    """
    Test that deleting documents at a specific creation time is a noop when there are no matching
    documents.
    """
    vertcol = create_timetravel_collection(arango_db, 'v')
    mergecol = create_timetravel_collection(arango_db, 'm', edge=True)
    edgecol = create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')
    
    att = ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', merge_collection='m', edge_collections=['e'])

    for col, edge in [(vertcol, False), (mergecol, True), (edgecol, True)]:
        actual_td, actual_expected = _prep_data_for_revert_tests(col.name, edge)
        col.import_bulk(actual_td)
        att.delete_created_documents(col.name, 101)
    
        check_docs(arango_db, actual_expected, col.name)

def test_delete_created_documents(arango_db):
    """
    Test deleting documents at a specific creation time.
    """
    vertcol = create_timetravel_collection(arango_db, 'v')
    mergecol = create_timetravel_collection(arango_db, 'm', edge=True)
    edgecol = create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')
    
    att = ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', merge_collection='m', edge_collections=['e'])

    for col, edge in [(vertcol, False), (mergecol, True), (edgecol, True)]:
        actual_td, actual_expected = _prep_data_for_revert_tests(col.name, edge)
        col.import_bulk(actual_td)
        att.delete_created_documents(col.name, 100)
        actual_expected = actual_expected[3:]
    
        check_docs(arango_db, actual_expected, col.name)

def test_delete_created_documents_fail_no_collection(arango_db):
    """
    Tests attempting to delete created documents on a non-existant collection.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'm', edge=True)
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')
    
    att = ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', merge_collection='m', edge_collections=['e'])

    check_exception(lambda: att.delete_created_documents('z', 100),
        ValueError, 'Collection z was not registered at initialization')

def test_undo_expire_documents_noop(arango_db):
    """
    Test that un-expiring documents at a specific expiration time is a noop when there are no
    matching documents.
    """
    vertcol = create_timetravel_collection(arango_db, 'v')
    edgecol = create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')
    
    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    for col, edge in [(vertcol, False), (edgecol, True)]:
        actual_td, actual_expected = _prep_data_for_revert_tests(col.name, edge)
        col.import_bulk(actual_td)
        att.undo_expire_documents(col.name, 301)
    
        check_docs(arango_db, actual_expected, col.name)

def test_undo_expire_documents(arango_db):
    """
    Test un-expiring documents at a specific creation time.
    """
    vertcol = create_timetravel_collection(arango_db, 'v')
    edgecol = create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')
    
    att = ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', edge_collections=['e'])

    for col, edge in [(vertcol, False), (edgecol, True)]:
        actual_td, actual_expected = _prep_data_for_revert_tests(col.name, edge)
        col.import_bulk(actual_td)
        att.undo_expire_documents(col.name, 300)
        actual_expected[0]['expired'] = 9007199254740991
        actual_expected[3]['expired'] = 9007199254740991
    
        check_docs(arango_db, actual_expected, col.name)

def test_undo_expire_documents_fail_no_collection(arango_db):
    """
    Tests attempting to delete created documents on a non-existant collection.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')
    
    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    check_exception(lambda: att.undo_expire_documents('a', 100),
        ValueError, 'Collection a was not registered at initialization')

def test_reset_last_version_noop(arango_db):
    """
    Test that resetting versions is a noop when there are no matching documents.
    """
    vertcol = create_timetravel_collection(arango_db, 'v')
    edgecol = create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')
    
    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    for col, edge in [(vertcol, False), (edgecol, True)]:
        actual_td, actual_expected = _prep_data_for_revert_tests(col.name, edge)
        col.import_bulk(actual_td)
        att.reset_last_version(col.name, '3', '2')
    
        check_docs(arango_db, actual_expected, col.name)

def test_reset_last_version(arango_db):
    """
    Test resetting the last versions on documents
    """
    vertcol = create_timetravel_collection(arango_db, 'v')
    edgecol = create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')
    
    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    for col, edge in [(vertcol, False), (edgecol, True)]:
        actual_td, actual_expected = _prep_data_for_revert_tests(col.name, edge)
        col.import_bulk(actual_td)
        att.reset_last_version(col.name, '2', '0')
        actual_expected[0]['last_version'] = '0'
        actual_expected[3]['last_version'] = '0'
        actual_expected[4]['last_version'] = '0'
    
        check_docs(arango_db, actual_expected, col.name)

def test_reset_last_version_fail_no_collection(arango_db):
    """
    Tests attempting to delete created documents on a non-existant collection.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')
    
    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    check_exception(lambda: att.reset_last_version('y', 'v2', 'v1'),
        ValueError, 'Collection y was not registered at initialization')

####################################
# Batch updater tests
####################################

def test_batch_noop_vertex(arango_db):
    """
    Test that running an update on a vertex collection with no updates does nothing.

    Also check collection name and collection type.
    """
    col = create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    b = att.get_batch_updater()
    assert b.get_collection() == 'v'
    assert b.is_edge is False
    assert b.count() == 0

    b.update()

    assert col.count() == 0
    assert b.count() == 0

def test_batch_noop_edge(arango_db):
    """
    Test that running an update on an edge collection with no updates does nothing.

    Also check collection name and collection type.

    Also check that merge collection isn't picked up accidentally
    """
    create_timetravel_collection(arango_db, 'v')
    col = create_timetravel_collection(arango_db, 'e', edge=True)
    create_timetravel_collection(arango_db, 'm', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', default_edge_collection='e', merge_collection='m')
    b = att.get_batch_updater(edge_collection_name='e')
    assert b.get_collection() == 'e'
    assert b.is_edge is True
    assert b.count() == 0

    b.update()

    assert col.count() == 0
    assert b.count() == 0

def test_batch_noop_merge(arango_db):
    """
    Test that running an update on a merge collection with no updates does nothing.

    Also check collection name and collection type.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    col = create_timetravel_collection(arango_db, 'm', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', default_edge_collection='e', merge_collection='m')
    b = att.get_batch_updater(edge_collection_name='m')
    assert b.get_collection() == 'm'
    assert b.is_edge is True
    assert b.count() == 0

    b.update()

    assert col.count() == 0
    assert b.count() == 0
    
def test_batch_fail_get_batch_updater(arango_db):
    """
    Test failure to get batch updater due ot unknown edge collection.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    
    check_exception(lambda: att.get_batch_updater('v'), ValueError,
        'Edge collection v was not registered at initialization')
    
def test_batch_create_vertices(arango_db):
    """
    Test creating 2 vertices in one batch.
    """
    col = create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')
    
    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    
    b = att.get_batch_updater()

    key = b.create_vertex('id1', 'ver1', 800, {'foo': 'bar'})
    assert key == 'id1_ver1'

    key = b.create_vertex('id2', 'ver2', 900, {'foo': 'bar1'})
    assert key == 'id2_ver2'

    assert col.count() == 0 # no verts should've been created yet
    assert b.count() == 2

    b.update()
    assert b.count() == 0

    expected = [
        {'_key': 'id1_ver1',
         '_id': 'v/id1_ver1',
         'created': 800,
         'expired': 9007199254740991,
         'first_version': 'ver1',
         'id': 'id1',
         'last_version': 'ver1',
         'foo': 'bar'},
        {'_key': 'id2_ver2',
         '_id': 'v/id2_ver2',
         'created': 900,
         'expired': 9007199254740991,
         'first_version': 'ver2',
         'id': 'id2',
         'last_version': 'ver2',
         'foo': 'bar1'}
    ]
    check_docs(arango_db, expected, 'v')

def test_batch_create_vertex_fail_not_vertex_collection(arango_db):
    """
    Test failing to add a vertex to a batch updater as the batch updater is for edges.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')

    b = att.get_batch_updater('e')

    check_exception(lambda: b.create_vertex('i', 'v', 6, {}), ValueError,
        'Batch updater is configured for an edge collection')

def test_batch_create_edges(arango_db):
    """
    Test creating 2 edges in one batch.
    """
    create_timetravel_collection(arango_db, 'v')
    col = create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    
    b = att.get_batch_updater('e')

    key = b.create_edge(
        'id1',
        # these 'nodes' are cheating - normally they'd be pulled from the db and have many
        # more fields, but I happen to know that just these two fields are needed.
        {'id': 'whee', '_id': 'v/1'},
        {'id': 'whoo', '_id': 'v/2'},
        'ver1',
        800)
    assert key == 'id1_ver1'

    key = b.create_edge(
        'id2',
        {'id': 'whee2', '_id': 'v/3'},
        {'id': 'whoo2', '_id': 'v/4'},
        'ver2',
        900,
        {'foo': 'bar1'})
    assert key == 'id2_ver2'

    assert col.count() == 0 # no edges should've been created yet
    assert b.count() == 2

    b.update()
    assert b.count() == 0

    expected = [
        {'_key': 'id1_ver1',
         '_id': 'e/id1_ver1',
         'from': 'whee',
         '_from': 'v/1',
         'to': 'whoo',
         '_to': 'v/2',
         'created': 800,
         'expired': 9007199254740991,
         'first_version': 'ver1',
         'id': 'id1',
         'last_version': 'ver1'},
        {'_key': 'id2_ver2',
         '_id': 'e/id2_ver2',
         'from': 'whee2',
         '_from': 'v/3',
         'to': 'whoo2',
         '_to': 'v/4',
         'created': 900,
         'expired': 9007199254740991,
         'first_version': 'ver2',
         'id': 'id2',
         'last_version': 'ver2',
         'foo': 'bar1'}
    ]
    check_docs(arango_db, expected, 'e')

def test_batch_create_mergeedge(arango_db):
    """
    Test creating a merge edge in one batch.

    Could be tests for calling multiple methods on merge edges but that seems like overkill.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    col = create_timetravel_collection(arango_db, 'm', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(
        arango_db, 'reg', 'v', default_edge_collection='e', merge_collection='m')
    
    b = att.get_batch_updater('m')

    key = b.create_edge(
        'id1',
        # these 'nodes' are cheating - normally they'd be pulled from the db and have many
        # more fields, but I happen to know that just these two fields are needed.
        {'id': 'whee', '_id': 'v/1'},
        {'id': 'whoo', '_id': 'v/2'},
        'ver1',
        800)
    assert key == 'id1_ver1'

    assert col.count() == 0 # no edges should've been created yet
    assert b.count() == 1

    b.update()
    assert b.count() == 0

    expected = [
        {'_key': 'id1_ver1',
         '_id': 'm/id1_ver1',
         'from': 'whee',
         '_from': 'v/1',
         'to': 'whoo',
         '_to': 'v/2',
         'created': 800,
         'expired': 9007199254740991,
         'first_version': 'ver1',
         'id': 'id1',
         'last_version': 'ver1'}
    ]
    check_docs(arango_db, expected, 'm')

def test_batch_create_edge_fail_not_edge_collection(arango_db):
    """
    Test failing to add a edge to a batch updater as the batch updater is for vertices.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')

    b = att.get_batch_updater()

    check_exception(lambda: b.create_edge('i', {}, {}, 'v', 6, {}), ValueError,
        'Batch updater is configured for a vertex collection')

def test_batch_set_last_version_on_vertex(arango_db):
    """
    Test setting the last version on vertices.
    """
    col = create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    expected = [{'_id': 'v/1', '_key': '1', 'id': 'foo', 'last_version': '1'},
                {'_id': 'v/2', '_key': '2', 'id': 'bar', 'last_version': '1'},
                {'_id': 'v/3', '_key': '3', 'id': 'baz', 'last_version': '1'},
                ]

    col.import_bulk(expected)

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    b = att.get_batch_updater()

    b.set_last_version_on_vertex('1', '2')
    b.set_last_version_on_vertex('2', '2')

    check_docs(arango_db, expected, 'v') # expect no changes

    assert b.count() == 2

    b.update()
    assert b.count() == 0

    expected = [{'_id': 'v/1', '_key': '1', 'id': 'foo', 'last_version': '2'},
                {'_id': 'v/2', '_key': '2', 'id': 'bar', 'last_version': '2'},
                {'_id': 'v/3', '_key': '3', 'id': 'baz', 'last_version': '1'},
                ]
    check_docs(arango_db, expected, 'v')

def test_batch_set_last_version_on_vertex_fail_not_vertex_collection(arango_db):
    """
    Test failing to set the last version on a vertex in a batch updater as the batch updater is
    for edges.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')

    b = att.get_batch_updater('e')

    check_exception(lambda: b.set_last_version_on_vertex('k', 'v'), ValueError,
        'Batch updater is configured for an edge collection')

def test_batch_set_last_version_on_edge(arango_db):
    """
    Test setting the last version on edges.
    """
    create_timetravel_collection(arango_db, 'v')
    col = create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    expected = [{'_id': 'e/1', '_key': '1', '_from': 'v/2', '_to': 'v/1', 'id': 'foo',
                 'last_version': '1'},
                {'_id': 'e/2', '_key': '2', '_from': 'v/2', '_to': 'v/1', 'id': 'bar',
                 'last_version': '1'},
                {'_id': 'e/3', '_key': '3', '_from': 'v/2', '_to': 'v/1', 'id': 'baz',
                 'last_version': '1'},
                ]

    col.import_bulk(expected)

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    b = att.get_batch_updater('e')

    # these 'edges' are cheating - normally they'd be pulled from the db and have many
    # more fields, but I happen to know that just these fields are needed.
    b.set_last_version_on_edge({'_key': '1', '_from': 'v/2', '_to': 'v/1'}, '2')
    b.set_last_version_on_edge({'_key': '2', '_from': 'v/2', '_to': 'v/1'}, '2')

    check_docs(arango_db, expected, 'e') # expect no changes

    assert b.count() == 2
    b.update()
    assert b.count() == 0

    expected = [{'_id': 'e/1', '_key': '1', '_from': 'v/2', '_to': 'v/1', 'id': 'foo',
                 'last_version': '2'},
                {'_id': 'e/2', '_key': '2', '_from': 'v/2', '_to': 'v/1', 'id': 'bar',
                 'last_version': '2'},
                {'_id': 'e/3', '_key': '3', '_from': 'v/2', '_to': 'v/1', 'id': 'baz',
                 'last_version': '1'},
                ]
    check_docs(arango_db, expected, 'e')

def test_batch_set_last_version_on_edge_fail_not_edge_collection(arango_db):
    """
    Test failing to set the last version on an edge in a batch updater as the batch updater is
    for vertices.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')

    b = att.get_batch_updater()

    check_exception(lambda: b.set_last_version_on_edge({}, '2'), ValueError,
        'Batch updater is configured for a vertex collection')

def test_batch_expire_vertex(arango_db):
    """
    Test expiring vertices.
    """
    col = create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    expected = [{'_id': 'v/1', '_key': '1', 'id': 'foo', 'expired': 1000},
                {'_id': 'v/2', '_key': '2', 'id': 'bar', 'expired': 1000},
                {'_id': 'v/3', '_key': '3', 'id': 'baz', 'expired': 1000},
                ]

    col.import_bulk(expected)

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    b = att.get_batch_updater()

    b.expire_vertex('1', 500)
    b.expire_vertex('2', 500)

    check_docs(arango_db, expected, 'v') # expect no changes

    assert b.count() == 2
    b.update()
    assert b.count() == 0

    expected = [{'_id': 'v/1', '_key': '1', 'id': 'foo', 'expired': 500},
                {'_id': 'v/2', '_key': '2', 'id': 'bar', 'expired': 500},
                {'_id': 'v/3', '_key': '3', 'id': 'baz', 'expired': 1000},
                ]
    check_docs(arango_db, expected, 'v')

def test_batch_expire_vertex_fail_not_vertex_collection(arango_db):
    """
    Test failing to expire a vertex in a batch updater as the batch updater is
    for edges.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')

    b = att.get_batch_updater('e')

    check_exception(lambda: b.expire_vertex('k', 1), ValueError,
        'Batch updater is configured for an edge collection')

def test_batch_expire_edge(arango_db):
    """
    Test expiring edges.
    """
    create_timetravel_collection(arango_db, 'v')
    col = create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    expected = [{'_id': 'e/1', '_key': '1', '_from': 'v/2', '_to': 'v/1', 'id': 'foo',
                 'expired': 1000},
                {'_id': 'e/2', '_key': '2', '_from': 'v/2', '_to': 'v/1', 'id': 'bar',
                 'expired': 1000},
                {'_id': 'e/3', '_key': '3', '_from': 'v/2', '_to': 'v/1', 'id': 'baz',
                 'expired': 1000},
                ]

    col.import_bulk(expected)

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')
    b = att.get_batch_updater('e')

    # these 'edges' are cheating - normally they'd be pulled from the db and have many
    # more fields, but I happen to know that just these fields are needed.
    b.expire_edge({'_key': '1', '_from': 'v/2', '_to': 'v/1'}, 500)
    b.expire_edge({'_key': '2', '_from': 'v/2', '_to': 'v/1'}, 500)

    check_docs(arango_db, expected, 'e') # expect no changes

    assert b.count() == 2
    b.update()
    assert b.count() == 0

    expected = [{'_id': 'e/1', '_key': '1', '_from': 'v/2', '_to': 'v/1', 'id': 'foo',
                 'expired': 500},
                {'_id': 'e/2', '_key': '2', '_from': 'v/2', '_to': 'v/1', 'id': 'bar',
                 'expired': 500},
                {'_id': 'e/3', '_key': '3', '_from': 'v/2', '_to': 'v/1', 'id': 'baz',
                 'expired': 1000},
                ]
    check_docs(arango_db, expected, 'e')

def test_batch_expire_edge_fail_not_edge_collection(arango_db):
    """
    Test failing to set the last version on an edge in a batch updater as the batch updater is
    for vertices.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', default_edge_collection='e')

    b = att.get_batch_updater()

    check_exception(lambda: b.expire_edge({}, 1), ValueError,
        'Batch updater is configured for a vertex collection')

####################################
# DB factory tests
####################################

def test_factory_get_registry_collection(arango_db):
    arango_db.create_collection('reg')

    fac = ArangoBatchTimeTravellingDBFactory(arango_db, 'reg')

    assert fac.get_registry_collection() == 'reg'

def test_factory_get_instance(arango_db):
    arango_db.create_collection('reg')

    fac = ArangoBatchTimeTravellingDBFactory(arango_db, 'reg')

    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'edef', edge=True)
    create_timetravel_collection(arango_db, 'e1', edge=True)
    create_timetravel_collection(arango_db, 'e2', edge=True)
    create_timetravel_collection(arango_db, 'm', edge=True)

    att = fac.get_instance('v', default_edge_collection='edef', edge_collections=['e1', 'e2'],
        merge_collection='m')

    assert att.get_registry_collection() == 'reg'
    assert att.get_vertex_collection() == 'v'
    assert att.get_default_edge_collection() == 'edef'
    assert att.get_edge_collections() == ['e1', 'e2', 'edef']
    assert att.get_merge_collection() == 'm'

def test_factory_fail_bad_registry_collection(arango_db):
    create_timetravel_collection(arango_db, 'r', edge=True)

    check_exception(
        lambda: ArangoBatchTimeTravellingDBFactory(arango_db, 'r'),
        ValueError, 'r is not a vertex collection')

def test_factory_get_registered_loads_empty(arango_db):
    """
    Test getting the set of registered loads when there are none to get from a db factory.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    att.register_load_start('ns2', 'v3', 700, 500, 400)

    fac = ArangoBatchTimeTravellingDBFactory(arango_db, 'reg')

    assert fac.get_registered_loads('ns1') == []

def test_factory_get_registered_loads(arango_db):
    """
    Test getting the set of registered loads from a db factory.
    """
    create_timetravel_collection(arango_db, 'v')
    create_timetravel_collection(arango_db, 'e', edge=True)
    arango_db.create_collection('reg')

    att = ArangoBatchTimeTravellingDB(arango_db, 'reg', 'v', edge_collections=['e'])

    att.register_load_start('ns1', 'v3', 700, 610, 400)
    att.register_load_start('ns1', 'v1', 500, 410, 300)
    att.register_load_complete('ns1', 'v1', 350)
    att.register_load_start('ns1', 'v2', 600, 510, 360)
    att.register_load_complete('ns1', 'v2', 390)
    att.register_load_start('ns2', 'v3', 700, 620, 400)

    expected = [
        {'_key': 'ns1_v3',
         '_id': 'reg/ns1_v3',
         'load_namespace': 'ns1',
         'load_version': 'v3',
         'load_timestamp': 700,
         'release_timestamp': 610,
         'start_time': 400,
         'completion_time': None,
         'state': 'in_progress',
         'vertex_collection': 'v',
         'merge_collection': None, 
         'edge_collections': ['e']
        },
        {'_key': 'ns1_v2',
         '_id': 'reg/ns1_v2',
         'load_namespace': 'ns1',
         'load_version': 'v2',
         'load_timestamp': 600,
         'release_timestamp': 510,
         'start_time': 360,
         'completion_time': 390,
         'state': 'complete',
         'vertex_collection': 'v',
         'merge_collection': None, 
         'edge_collections': ['e']
        },
        {'_key': 'ns1_v1',
         '_id': 'reg/ns1_v1',
         'load_namespace': 'ns1',
         'load_version': 'v1',
         'load_timestamp': 500,
         'release_timestamp': 410,
         'start_time': 300,
         'completion_time': 350,
         'state': 'complete',
         'vertex_collection': 'v',
         'merge_collection': None, 
         'edge_collections': ['e']
        },
    ]

    fac = ArangoBatchTimeTravellingDBFactory(arango_db, 'reg')
    got = fac.get_registered_loads('ns1')
    assert got == expected

    expected = [
        {'_key': 'ns2_v3',
         '_id': 'reg/ns2_v3',
         'load_namespace': 'ns2',
         'load_version': 'v3',
         'load_timestamp': 700,
         'release_timestamp': 620,
         'start_time': 400,
         'completion_time': None,
         'state': 'in_progress',
         'vertex_collection': 'v',
         'merge_collection': None, 
         'edge_collections': ['e']
        },
    ]
    got = fac.get_registered_loads('ns2')
    assert got == expected

####################################
# Helper funcs
####################################

def _check_no_fake_changes(arango_db):
    fake_nodes = [{'_key': '1', '_id': 'fake/1', 'id': 'bat', 'created': 100, 'expired': 9000},
                  {'_key': '2', '_id': 'fake/2', 'id': 'baz', 'created': 100, 'expired': 9000},
                  ]
    check_docs(arango_db, fake_nodes, 'fake')