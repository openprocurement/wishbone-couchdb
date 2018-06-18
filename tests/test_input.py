from gevent import monkey; monkey.patch_all()
import pytest
import couchdb
from wishbone.event import Event
from wishbone.actor import ActorConfig
from wishbone.utils.test import getter

from wishbonecouchdb import CouchdbPoller


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


def test_couchdb_input(db):
    config = ActorConfig('couchdbpoller', 100, 1, {}, "")
    module = CouchdbPoller(
        config,
        couchdb_url="{}/{}".format(couchdb_url, DB_NAME),
    )

    module.pool.queue.outbox.disableFallThrough()
    module.start()
    module.couchdb.save({"data": "data", "id": "id"})
    one = getter(module.pool.queue.outbox)
    assert one.get().get('data') == "data"
