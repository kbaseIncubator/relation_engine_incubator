# TODO start a new arango instance as part of the tests so:
# a) we remove chance of data corruption and 
# b) we don't leave test data around

# TODO more tests

from relation_engine.database.batch_time_travelling_db import ArangoBatchTimeTravellingDB
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

def test_get_vertex(arango_db):
    col_name = 'verts'
    col = arango_db.create_collection(col_name)

    # it's assumed that given an id and a timestamp there's <= 1 match in the collection
    col.import_bulk([{'_key': '1', 'id': 'foo', 'created': 100, 'expired': 600},
                     {'_key': '2', 'id': 'bar', 'created': 100, 'expired': 200},
                     {'_key': '3', 'id': 'bar', 'created': 201, 'expired': 300}, # target
                     {'_key': '4', 'id': 'bar', 'created': 301, 'expired': 400},
                     ])

    att = ArangoBatchTimeTravellingDB(arango_db, default_vertex_collection=col_name)

    ret = att.get_vertex('bar', 201)
    assert ret == {'_key': '3', 'id': 'bar', 'created': 201, 'expired': 300}

    ret = att.get_vertex('bar', 300)
    assert ret == {'_key': '3', 'id': 'bar', 'created': 201, 'expired': 300}

    ret = att.get_vertex('foo', 250)
    assert ret == {'_key': '1', 'id': 'foo', 'created': 100, 'expired': 600}

    assert att.get_vertex('bar', 99) == None
    assert att.get_vertex('bar', 401) == None

    col.insert({'_key': '5', 'id': 'bar', 'created': 150, 'expired': 250})

    try:
        att.get_vertex('bar', 200)
    except ValueError as e:
        assert e.args[0] == 'db contains > 1 vertex for id bar, timestamp 200, collection verts'

def test_save_vertex(arango_db):
    col_name = 'verts'
    arango_db.create_collection(col_name)
    att = ArangoBatchTimeTravellingDB(arango_db, default_vertex_collection=col_name)

    att.save_vertex('myid', 'load-ver1', 500, {'science': 'yes!'})
    att.save_vertex('myid2', 'load-ver1', 600, {'science': 'yes indeed!'})

    ret = att.get_vertex('myid', 600)
    assert ret == {'_key': 'myid_load-ver1',
                   'created': 500,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid',
                   'last_version': 'load-ver1',
                   'science': 'yes!'}
    
    ret = att.get_vertex('myid2', 600)
    assert ret == {'_key': 'myid2_load-ver1',
                   'created': 600,
                   'expired': 9007199254740991,
                   'first_version': 'load-ver1',
                   'id': 'myid2',
                   'last_version': 'load-ver1',
                   'science': 'yes indeed!'}

