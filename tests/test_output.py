from gevent import monkey; monkey.patch_all()
import pytest
import couchdb
from wishbone.event import Event
from wishbone.actor import ActorConfig
from wishbone.utils.test import getter
from gevent import sleep

from wishbonecouchdb import CouchdbPuller


DB_HOST = "admin:admin@127.0.0.1"
DB_PORT = "5984"
DB_NAME = "test"
couchdb_url = 'http://{}:{}'.format(DB_HOST, DB_PORT)



@pytest.fixture(scope='function')
def db(request):
    SERVER = couchdb.Server(couchdb_url)
    def delete():
        if DB_NAME in SERVER:
            del SERVER[DB_NAME]

    if DB_NAME in SERVER:
        delete()
    SERVER.create(DB_NAME)
    request.addfinalizer(delete)


def test_couchdb_output(db):
    config = ActorConfig('couchdbpoller', 100, 1, {}, "")
    module = CouchdbPuller(
        config,
        couchdb_url="{}/{}".format(couchdb_url, DB_NAME),
        bulk=1
    )

    module.pool.queue.inbox.disableFallThrough()
    module.start()
    
    data = {"data": "data", "_id": "test_doc"}
    e = Event(data)
    module.pool.queue.inbox.put(e)
    doc = module.couchdb.get('test_doc')
    retry = 5
    while doc is None and retry:
        doc = module.couchdb.get('test_doc')
        sleep(0.1)
        retry -= 1
    assert doc
    assert doc['data'] == 'data'
