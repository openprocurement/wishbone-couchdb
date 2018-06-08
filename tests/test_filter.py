from gevent import monkey; monkey.patch_all()
import pytest
import mock
import couchdb
from couchdb.design import ViewDefinition
from wishbone.event import Event
from wishbone.actor import ActorConfig
from gevent import sleep

from wishbonecouchdb import CouchdbFilter

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

    delete()
    db = SERVER.create(DB_NAME)
    view = ViewDefinition('test', 'all', """
        function (doc) { emit(doc._id, doc._id); }
    """)
    view.sync(db)
    request.addfinalizer(delete)

def test_coucdb_filter(db):
    config = ActorConfig('couchdbfilter', 100, 1, {}, "")
    module = CouchdbFilter(
        actor_config=config,
        couchdb_url="{}/{}".format(couchdb_url, DB_NAME),
        view_path="test/all",
        filter_key='._id',
        filter_value='if .mode == "test" then "" else ._id end',
        op="eq"
    )
    data = {"data": "data", "_id": "test_doc", "id": "test_doc"}
    module.couchdb.save(data)
    module.logging.debug = mock.MagicMock()

    module.pool.queue.outbox.disableFallThrough()
    module.pool.queue.inbox.disableFallThrough()
    module.start()

    e = Event(data)
    module.pool.queue.inbox.put(e)
    sleep(1)
    module.logging.debug.assert_called_with("Event from inbox {}".format(e))

    data = module.couchdb.get("test_doc")
    data["mode"] = "test"
    module.couchdb.save(data)
    e = Event(data)
    module.pool.queue.inbox.put(e)
    sleep(1)
    module.logging.debug.assert_called_with(
        'Skipped test_doc by filter. Op: eq old: test_doc new ')
