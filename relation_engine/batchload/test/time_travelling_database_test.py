# TODO start a new arango instance as part of the tests so:
# a) we remove chance of data corruption and 
# b) we don't leave test data around

# TODO more tests

from relation_engine.batchload.time_travelling_database import ArangoBatchTimeTravellingDB
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

def test_get_vertex_collection(arango_db):
    arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)

    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')
    assert att.get_vertex_collection() == 'v'

def test_get_default_edge_collection(arango_db):
    arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)

    att = ArangoBatchTimeTravellingDB(arango_db, 'v', edge_collections=['e'])
    assert att.get_default_edge_collection() is None

    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')
    assert att.get_default_edge_collection() is 'e'

def test_get_edge_collections(arango_db):
    arango_db.create_collection('v')
    arango_db.create_collection('e1', edge=True)
    arango_db.create_collection('e2', edge=True)
    arango_db.create_collection('e3', edge=True)

    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e1')
    assert adbtt.get_edge_collections() == ['e1']

    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'v', edge_collections=['e2', 'e2', 'e1'])
    assert adbtt.get_edge_collections() == ['e1', 'e2']

    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e3',
        edge_collections=['e2', 'e2', 'e1', 'e3'])
    assert adbtt.get_edge_collections() == ['e1', 'e2', 'e3']

def test_init_fail_no_edge_collections(arango_db):
    arango_db.create_collection('v')

    _check_exception(lambda: ArangoBatchTimeTravellingDB(arango_db, 'v'), ValueError,
        'At least one edge collection must be specified')

def test_init_fail_bad_vertex_collection(arango_db):
    arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)

    _check_exception(
        lambda: ArangoBatchTimeTravellingDB(arango_db, 'e', default_edge_collection='e'),
        ValueError, 'e is not a vertex collection')


def test_init_fail_bad_edge_collection(arango_db):
    arango_db.create_collection('v')
    col_name = 'edge'
    arango_db.create_collection(col_name)

    _check_exception(
        lambda: ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection=col_name),
        ValueError, 'edge is not an edge collection')

    _check_exception(
        lambda: ArangoBatchTimeTravellingDB(arango_db, 'v', edge_collections=[col_name]),
        ValueError, 'edge is not an edge collection')

def test_fail_no_default_edge_collection(arango_db):
    """
    Really should test this for all methods but that seems like a lot of tests for the same
    chunk of factored out code
    """
    arango_db.create_collection('v')
    col_name = 'edge'
    arango_db.create_collection(col_name, edge=True)
    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'v', edge_collections=[col_name])

    _check_exception(lambda:  adbtt.get_edges(['id'], 3), ValueError,
        'No default edge collection specified, must specify edge collection')

def test_fail_no_such_edge_collection(arango_db):
    """
    Really should test this for all methods but that seems like a lot of tests for the same
    chunk of factored out code

    Also tests that duplicate collection names don't cause a problem
    """
    arango_db.create_collection('v')
    arango_db.create_collection('e1', edge=True)
    arango_db.create_collection('e2', edge=True)
    arango_db.create_collection('e3', edge=True)
    adbtt = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e1',
        edge_collections=['e2', 'e3', 'e2', 'e1'])

    _check_exception(lambda:  adbtt.expire_edge('id', 3, 'e4'), ValueError,
        'Edge collection e4 was not registered at initialization')

def test_get_vertices(arango_db):
    """
    Tests that getting a vertex returns the correct vertex. In particular checks for OB1 errors.
    """

    col_name = 'verts'
    col = arango_db.create_collection(col_name)
    arango_db.create_collection('e', edge=True)

    # it's assumed that given an id and a timestamp there's <= 1 match in the collection
    col.import_bulk([{'_key': '1', 'id': 'foo', 'created': 100, 'expired': 600},
                     {'_key': '2', 'id': 'bar', 'created': 100, 'expired': 200},
                     {'_key': '3', 'id': 'bar', 'created': 201, 'expired': 300},
                     {'_key': '4', 'id': 'bar', 'created': 301, 'expired': 400},
                     ])

    att = ArangoBatchTimeTravellingDB(arango_db, col_name, edge_collections=['e'])

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

    _check_exception(lambda:  att.get_vertices(['bar'], 200), ValueError,
        'db contains > 1 document for id bar, timestamp 200, collection verts')

def test_get_edges(arango_db):
    """
    Tests that getting a edge returns the correct edge. In particular checks for OB1 errors.
    """

    arango_db.create_collection('v')
    col_name = 'edges'
    col = arango_db.create_collection(col_name, edge=True)

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

    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection=col_name)

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

    _check_exception(lambda:  att.get_edges(['bar'], 200), ValueError,
        'db contains > 1 document for id bar, timestamp 200, collection edges')

def test_save_vertex(arango_db):
    """
    Tests saving a vertex and retrieving the new vertex.
    """

    col_name = 'verts'
    arango_db.create_collection(col_name)
    arango_db.create_collection('e', edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, col_name, default_edge_collection='e')

    k = att.save_vertex('myid', 'load-ver1', 500, {'science': 'yes!'})
    assert k == 'myid_load-ver1'
    k = att.save_vertex('myid2', 'load-ver1', 600, {'science': 'yes indeed!'})
    assert k == 'myid2_load-ver1'

    ret = att.get_vertices(['myid'], 600).get('myid')
    assert ret == {'_key': 'myid_load-ver1',
                   '_id': 'verts/myid_load-ver1',
                   'created': 500,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid',
                   'last_version': 'load-ver1',
                   'science': 'yes!'}
    
    ret = att.get_vertices(['myid2'], 600).get('myid2')
    assert ret == {'_key': 'myid2_load-ver1',
                   '_id': 'verts/myid2_load-ver1',
                   'created': 600,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid2',
                   'last_version': 'load-ver1',
                   'science': 'yes indeed!'}


def test_save_edge(arango_db):
    """
    Tests saving an edge and retrieving the new edge.
    """

    arango_db.create_collection('v')
    col_name = 'edges'
    arango_db.create_collection(col_name, edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', edge_collections=[col_name])

    k = att.save_edge(
        'myid',
        {'id': 'whee', '_id': 'fake/1'},
        {'id': 'whoo', '_id': 'fake/2'},
        'load-ver1',
        500,
        edge_collection=col_name)
    assert k == 'myid_load-ver1'
    k = att.save_edge(
        'myid2',
        {'id': 'whee', '_id': 'fake/1'},
        {'id': 'whoo', '_id': 'fake/2'},
        'load-ver1',
        600,
        {'science': 'yes indeed!'},
        edge_collection=col_name)
    assert k == 'myid2_load-ver1'

    ret = att.get_edges(['myid'], 600, edge_collection=col_name).get('myid')
    assert ret == {'_key': 'myid_load-ver1',
                   '_id': 'edges/myid_load-ver1',
                   '_from': 'fake/1',
                   '_to': 'fake/2',
                   'from': 'whee',
                   'to': 'whoo',
                   'created': 500,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid',
                   'last_version': 'load-ver1'}
    
    ret = att.get_edges(['myid2'], 600, edge_collection=col_name).get('myid2')
    assert ret == {'_key': 'myid2_load-ver1',
                   '_id': 'edges/myid2_load-ver1',
                   '_from': 'fake/1',
                   '_to': 'fake/2',
                   'from': 'whee',
                   'to': 'whoo',
                   'created': 600,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid2',
                   'last_version': 'load-ver1',
                   'science': 'yes indeed!'}

def test_set_last_version_on_vertex(arango_db):
    """
    Tests setting the `last_version` field on a vertex, and specifically that the correct
    vertex is modified.
    """

    col_name = 'verts'
    arango_db.create_collection(col_name)
    arango_db.create_collection('e', edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, col_name, edge_collections=['e'])

    key = att.save_vertex('myid', 'load-ver1', 500, {'science': 'yes!'})
    _ = att.save_vertex('myid1', 'load-ver1', 500, {'science': 'yes!'})

    att.set_last_version_on_vertex(key, 'load-ver42')

    ret = att.get_vertices(['myid'], 600).get('myid')
    assert ret == {'_key': 'myid_load-ver1',
                   '_id': 'verts/myid_load-ver1',
                   'created': 500,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid',
                   'last_version': 'load-ver42',
                   'science': 'yes!'}

    ret = att.get_vertices(['myid1'], 600).get('myid1')
    assert ret == {'_key': 'myid1_load-ver1',
                   '_id': 'verts/myid1_load-ver1',
                   'created': 500,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid1',
                   'last_version': 'load-ver1',
                   'science': 'yes!'}

def test_set_last_version_on_edge(arango_db):
    """
    Tests setting the `last_version` field on an edge, and specifically that the correct
    edge is modified.
    """

    arango_db.create_collection('v')
    col_name = 'edges'
    arango_db.create_collection(col_name, edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection=col_name)

    key = att.save_edge(
        'myid',
        {'id': 'whee', '_id': 'fake/1'},
        {'id': 'whoo', '_id': 'fake/2'},
        'load-ver1',
        500)
    _ = att.save_edge(
        'myid2',
        {'id': 'whee', '_id': 'fake/1'},
        {'id': 'whoo', '_id': 'fake/2'},
        'load-ver1',
        600,
        {'science': 'yes indeed!'})

    att.set_last_version_on_edge(key, 'load-ver42')

    ret = att.get_edges(['myid'], 600).get('myid')
    assert ret == {'_key': 'myid_load-ver1',
                   '_id': 'edges/myid_load-ver1',
                   '_from': 'fake/1',
                   '_to': 'fake/2',
                   'from': 'whee',
                   'to': 'whoo',
                   'created': 500,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid',
                   'last_version': 'load-ver42'}
    
    ret = att.get_edges(['myid2'], 600).get('myid2')
    assert ret == {'_key': 'myid2_load-ver1',
                   '_id': 'edges/myid2_load-ver1',
                   '_from': 'fake/1',
                   '_to': 'fake/2',
                   'from': 'whee',
                   'to': 'whoo',
                   'created': 600,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid2',
                   'last_version': 'load-ver1',
                   'science': 'yes indeed!'}

def _setup_expire_vertex(arango_db):

    vert_col_name = 'verts'
    edge1_col_name = 'edges1'
    edge2_col_name = 'edges2'
    vert_col = arango_db.create_collection(vert_col_name)
    fake_col = arango_db.create_collection('fake')
    edge1_col = arango_db.create_collection(edge1_col_name, edge=True)
    edge2_col = arango_db.create_collection(edge2_col_name, edge=True)

    vert_col.import_bulk([
        {'_key': '1', '_id': 'verts/1', 'id': 'foo', 'created': 100, 'expired': 9000},
        {'_key': '2', '_id': 'verts/2', 'id': 'bar', 'created': 100, 'expired': 9000},
        ])
    fake_nodes = [
        {'_key': '1', '_id': 'fake/1', 'id': 'bat', 'created': 100, 'expired': 9000},
        {'_key': '2', '_id': 'fake/2', 'id': 'baz', 'created': 100, 'expired': 9000},
        ]
    fake_col.import_bulk(fake_nodes)
    
    edges1 = [{'_key': 'e1_1', '_id': 'edges1/e1_1', '_from': 'verts/1', '_to': 'fake/1',
               'created': 100, 'expired': 9000},
              {'_key': 'e1_2', '_id': 'edges1/e1_2', '_from': 'fake/2', '_to': 'verts/1',
               'created': 100, 'expired': 9000},
              {'_key': 'e1_3', '_id': 'edges1/e1_3', '_from': 'verts/2', '_to': 'fake/1',
               'created': 100, 'expired': 9000},
              {'_key': 'e1_4', '_id': 'edges1/e1_4', '_from': 'fake/2', '_to': 'verts/2',
               'created': 100, 'expired': 9000}, 
              ]

    edge1_col.import_bulk(edges1)

    edges2 = [{'_key': 'e2_1', '_id': 'edges2/e2_1', '_from': 'verts/1', '_to': 'fake/1',
               'created': 100, 'expired': 9000},
              {'_key': 'e2_2', '_id': 'edges2/e2_2', '_from': 'fake/2', '_to': 'verts/1',
               'created': 100, 'expired': 9000},
              {'_key': 'e2_3', '_id': 'edges2/e2_3', '_from': 'verts/2', '_to': 'fake/1',
               'created': 100, 'expired': 9000},
              {'_key': 'e2_4', '_id': 'edges2/e2_4', '_from': 'fake/2', '_to': 'verts/2',
               'created': 100, 'expired': 9000}, 
              ]

    edge2_col.import_bulk(edges2)

    return edges1, edges2

def test_expire_vertex_single_vertex(arango_db):
    """
    Tests that given a network of nodes and edges where the edges are in 2 collections and the
    nodes are in a single collection, expiring a single vertex updates only that vertex.
    No other vertices or edges should be modified.

    See _setup_set_node_expired for the test setup.
    """

    edges1, edges2 = _setup_expire_vertex(arango_db)
    att = ArangoBatchTimeTravellingDB(arango_db, 'verts', edge_collections=['edges1', 'edges2'])

    att.expire_vertex('1', 500)

    ret = att.get_vertices(['foo'], 200).get('foo')
    assert ret == {'_key': '1', '_id': 'verts/1', 'id': 'foo', 'created': 100, 'expired': 500}
    ret = att.get_vertices(['bar'], 200).get('bar')
    assert ret == {'_key': '2', '_id': 'verts/2', 'id': 'bar', 'created': 100, 'expired': 9000}

    _check_no_fake_changes(arango_db)

    _check_docs(arango_db, edges1, 'edges1')
    _check_docs(arango_db, edges2, 'edges2')


def test_expire_edge(arango_db):
    """
    Tests expiring an edge, and specifically that the correct edge is modified.
    """

    arango_db.create_collection('v')
    col_name = 'edges'
    arango_db.create_collection(col_name, edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection=col_name)

    key = att.save_edge(
        'myid',
        {'id': 'whee', '_id': 'fake/1'},
        {'id': 'whoo', '_id': 'fake/2'},
        'load-ver1',
        500)
    _ = att.save_edge(
        'myid2',
        {'id': 'whee', '_id': 'fake/1'},
        {'id': 'whoo', '_id': 'fake/2'},
        'load-ver1',
        600,
        {'science': 'yes indeed!'})

    att.expire_edge(key, 2000)

    ret = att.get_edges(['myid'], 600).get('myid')
    assert ret == {'_key': 'myid_load-ver1',
                   '_id': 'edges/myid_load-ver1',
                   '_from': 'fake/1',
                   '_to': 'fake/2',
                   'from': 'whee',
                   'to': 'whoo',
                   'created': 500,
                   'expired': 2000,
                   'first_version': 'load-ver1',
                   'id': 'myid',
                   'last_version': 'load-ver1'}
    
    ret = att.get_edges(['myid2'], 600).get('myid2')
    assert ret == {'_key': 'myid2_load-ver1',
                   '_id': 'edges/myid2_load-ver1',
                   '_from': 'fake/1',
                   '_to': 'fake/2',
                   'from': 'whee',
                   'to': 'whoo',
                   'created': 600,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid2',
                   'last_version': 'load-ver1',
                   'science': 'yes indeed!'}

def test_expire_extant_vertices_without_last_version(arango_db):
    """
    Tests expiring vertices that exist at a specfic time without a given last version.
    """
    col_name = 'verts'
    col = arango_db.create_collection(col_name)
    arango_db.create_collection('e', edge=True)

    test_data = [
        {'_key': '0', 'id': 'baz', 'created': 100, 'expired': 300, 'last_version': '2'},
        {'_key': '1', 'id': 'foo', 'created': 100, 'expired': 600, 'last_version': '1'},
        {'_key': '2', 'id': 'bar', 'created': 100, 'expired': 200, 'last_version': '1'},
        {'_key': '3', 'id': 'bar', 'created': 201, 'expired': 300, 'last_version': '2'},
        {'_key': '4', 'id': 'bar', 'created': 301, 'expired': 400, 'last_version': '2'},
        ]
    col.import_bulk(test_data)

    att = ArangoBatchTimeTravellingDB(arango_db, col_name, default_edge_collection='e')

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

    _check_docs(arango_db, expected, col_name)

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

    _check_docs(arango_db, expected, col_name)

def test_expire_extant_edges_without_last_version(arango_db):
    """
    Tests expiring egdes that exist at a specfic time without a given last version.
    """
    arango_db.create_collection('v')
    col_name = 'edges'
    col = arango_db.create_collection(col_name, edge=True)

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

    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection=col_name)

    # test 1
    att.expire_extant_edges_without_last_version(100, "2")

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

    _check_docs(arango_db, expected, col_name)

    # test 2
    col.delete_match({})
    col.import_bulk(test_data)

    att.expire_extant_edges_without_last_version(299, "1")

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

    _check_docs(arango_db, expected, col_name)

####################################
# Batch updater tests
####################################

def test_batch_noop_vertex(arango_db):
    """
    Test that running an update on a vertex collection with no updates does nothing.

    Also check collection name and collection type.
    """
    col = arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')
    b = att.get_batch_updater()
    assert b.get_collection() == 'v'
    assert b.is_edge is False

    b.update()

    assert col.count() == 0

def test_batch_noop_edge(arango_db):
    """
    Test that running an update on an edge collection with no updates does nothing.

    Also check collection name and collection type.
    """
    arango_db.create_collection('v')
    col = arango_db.create_collection('e', edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')
    b = att.get_batch_updater(edge_collection_name='e')
    assert b.get_collection() == 'e'
    assert b.is_edge is True

    b.update()

    assert col.count() == 0
    
def test_batch_fail_get_batch_updater(arango_db):
    """
    Test failure to get batch updater due ot unknown edge collection.
    """
    arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')
    
    _check_exception(lambda: att.get_batch_updater('v'), ValueError,
        'Edge collection v was not registered at initialization')
    
def test_batch_create_vertices(arango_db):
    """
    Test creating 2 vertices in one batch.
    """
    col = arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')
    
    b = att.get_batch_updater()

    key = b.create_vertex('id1', 'ver1', 800, {'foo': 'bar'})
    assert key == 'id1_ver1'

    key = b.create_vertex('id2', 'ver2', 900, {'foo': 'bar1'})
    assert key == 'id2_ver2'

    assert col.count() == 0 # no verts should've been created yet

    b.update()

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
    _check_docs(arango_db, expected, 'v')

def test_batch_create_vertex_fail_not_vertex_collection(arango_db):
    """
    Test failing to add a vertex to a batch updater as the batch updater is for edges.
    """
    arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')

    b = att.get_batch_updater('e')

    _check_exception(lambda: b.create_vertex('i', 'v', 6, {}), ValueError,
        'Batch updater is configured for an edge collection')

def test_batch_create_edges(arango_db):
    """
    Test creating 2 edges in one batch.
    """
    col = arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')
    
    b = att.get_batch_updater('e')

    key = b.create_edge(
        'id1',
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

    b.update()

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
    _check_docs(arango_db, expected, 'e')

def test_batch_create_edge_fail_not_edge_collection(arango_db):
    """
    Test failing to add a edge to a batch updater as the batch updater is for vertices.
    """
    arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')

    b = att.get_batch_updater()

    _check_exception(lambda: b.create_edge('i', {}, {}, 'v', 6, {}), ValueError,
        'Batch updater is configured for a vertex collection')

def test_batch_set_last_version_on_vertex(arango_db):
    """
    Test setting the last version on vertices.
    """
    col = arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)

    expected = [{'_id': 'v/1', '_key': '1', 'id': 'foo', 'last_version': '1'},
                {'_id': 'v/2', '_key': '2', 'id': 'bar', 'last_version': '1'},
                {'_id': 'v/3', '_key': '3', 'id': 'baz', 'last_version': '1'},
                ]

    col.import_bulk(expected)

    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')
    b = att.get_batch_updater()

    b.set_last_version_on_vertex('1', '2')
    b.set_last_version_on_vertex('2', '2')

    _check_docs(arango_db, expected, 'v') # expect no changes

    b.update()

    expected = [{'_id': 'v/1', '_key': '1', 'id': 'foo', 'last_version': '2'},
                {'_id': 'v/2', '_key': '2', 'id': 'bar', 'last_version': '2'},
                {'_id': 'v/3', '_key': '3', 'id': 'baz', 'last_version': '1'},
                ]
    _check_docs(arango_db, expected, 'v')

def test_batch_set_last_version_on_vertex_fail_not_vertex_collection(arango_db):
    """
    Test failing to set the last version on a vertex in a batch updater as the batch updater is
    for edges.
    """
    arango_db.create_collection('v')
    arango_db.create_collection('e', edge=True)
    att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')

    b = att.get_batch_updater('e')

    _check_exception(lambda: b.set_last_version_on_vertex('k', 'v'), ValueError,
        'Batch updater is configured for an edge collection')

# def test_batch_set_last_version_on_edge(arango_db):
#     """
#     Test setting the last version on edges.
#     """
#     arango_db.create_collection('v')
#     col = arango_db.create_collection('e', edge=True)

#     expected = [{'_id': 'e/1', '_key': '1', '_from': 'v/2', '_to': 'v/1', 'id': 'foo',
#                  'last_version': '1'},
#                 {'_id': 'e/2', '_key': '2', '_from': 'v/2', '_to': 'v/1', 'id': 'bar',
#                  'last_version': '1'},
#                 {'_id': 'e/3', '_key': '3', '_from': 'v/2', '_to': 'v/1', 'id': 'baz',
#                  'last_version': '1'},
#                 ]

#     col.import_bulk(expected)

#     att = ArangoBatchTimeTravellingDB(arango_db, 'v', default_edge_collection='e')
#     b = att.get_batch_updater('e')

#     b.set_last_version_on_vertex_or_edge('1', '2')
#     b.set_last_version_on_vertex_or_edge('2', '2')

#     _check_docs(arango_db, expected, 'e') # expect no changes

#     b.update()

#     expected = [{'_id': 'e/1', '_key': '1', '_from': 'v/2', '_to': 'v/1', 'id': 'foo',
#                  'last_version': '2'},
#                 {'_id': 'e/2', '_key': '2', '_from': 'v/2', '_to': 'v/1', 'id': 'bar',
#                  'last_version': '2'},
#                 {'_id': 'e/3', '_key': '3', '_from': 'v/2', '_to': 'v/1', 'id': 'baz',
#                  'last_version': '1'},
#                 ]
#     _check_docs(arango_db, expected, 'e')

####################################
# Helper funcs
####################################

def _check_docs(arango_db, docs, collection):
    col = arango_db.collection(collection)
    for d in docs:
        doc = col.get(d['_key'])
        del doc['_rev']
        assert d == doc

def _check_no_fake_changes(arango_db):
    fake_nodes = [{'_key': '1', '_id': 'fake/1', 'id': 'bat', 'created': 100, 'expired': 9000},
                  {'_key': '2', '_id': 'fake/2', 'id': 'baz', 'created': 100, 'expired': 9000},
                  ]
    _check_docs(arango_db, fake_nodes, 'fake')

def _check_exception(action, exception, message):
    try:
        action()
        assert 0, "Expected exception"
    except exception as e:
        assert e.args[0] == message
