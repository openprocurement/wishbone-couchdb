from gevent import monkey; monkey.patch_all()
import pytest
import mock
import couchdb
from couchdb.design import ViewDefinition
from wishbone.event import Event
from wishbone.actor import ActorConfig
from wishbone.utils.test import getter
from gevent import sleep

from wishbonecouchdb import ViewFilter, JQFilter

DB_HOST = "admin:admin@127.0.0.1"
DB_PORT = "5984"
DB_NAME = "test"
SERVER_URL = 'http://{}:{}'.format(DB_HOST, DB_PORT)


@pytest.fixture(scope='function')
def db(request):
    SERVER = couchdb.Server(SERVER_URL)
    def delete():
        if DB_NAME in SERVER:
            del SERVER[DB_NAME]

    delete()
    db = SERVER.create(DB_NAME)
    view = ViewDefinition('index', 'by_date',
    map_fun="""function (doc) { emit(doc.id, doc.date); }""")
    view.sync(db)
    request.addfinalizer(delete)


def test_coucdb_filter(db):
    config = ActorConfig('couchdbfilter', 100, 1, {}, "")
    module = ViewFilter(
        actor_config=config,
        couchdb_url="{}/{}".format(SERVER_URL, DB_NAME),
        view="index/by_date",
        view_expression='.id',
        conditions=[
            {
                'name': "Date filter",
                'queue': 'outbox',
                'expression': '.[0].value == .[1].date'
            }
        ]
    )
    event_data = {"date": "date", "_id": "id", "id": "id"}
    module.couchdb.save(event_data)

    module.pool.queue.outbox.disableFallThrough()
    module.pool.queue.inbox.disableFallThrough()
    module.start()

    e = Event(event_data)
    module.pool.queue.inbox.put(e)

    one = getter(module.pool.queue.outbox)
    assert one == event_data


def test_jq_filter():
    config = ActorConfig('jq', 100, 1, {}, "")
    module = JQFilter(
        actor_config=config,
        conditions=[
            {
                'name': "Doc mode filter",
                'queue': 'test_docs',
                'expression': '.mode == "test"'
            }
        ]
    )
    event_data = {"date": "date", "_id": "id", "id": "id", "mode": "test"}

    module.pool.queue.test_docs.disableFallThrough()
    module.pool.queue.inbox.disableFallThrough()
    module.start()

    e = Event(event_data)
    module.pool.queue.inbox.put(e)

    one = getter(module.pool.queue.test_docs)
    assert one.get() == event_data


def test_jq_nomatch():
    config = ActorConfig('jq', 100, 1, {}, "")
    module = JQFilter(
        actor_config=config,
        conditions=[
            {
                'name': "Doc mode filter",
                'queue': 'test_docs',
                'expression': '.mode == "test"'
            }
        ]
    )
    event_data = {"date": "date", "_id": "id", "id": "id"}

    module.pool.queue.test_docs.disableFallThrough()
    module.pool.queue.inbox.disableFallThrough()
    module.start()

    e = Event(event_data)
    module.pool.queue.inbox.put(e)

    with pytest.raises(Exception) as e:
        getter(module.pool.queue.test_docs)
        assert e.message == 'No event from queue'


def test_jq_match():
    config = ActorConfig('jq', 100, 1, {}, "")
    module = JQFilter(
        actor_config=config,
        conditions=[
            {
                'name': "Test match title",
                'queue': 'no_match',
                'expression': '.title | test(".+test.+"; "i")'
            },
            {
                'name': "Test unicode match title",
                'queue': 'no_match',
                'expression': '.title | test(".+тест.+"; "i")'
            },
            {
                'name': "Rest",
                'queue': 'outbox',
                'expression': '.'
            }
        ]
    )

    module.pool.queue.outbox.disableFallThrough()
    module.pool.queue.inbox.disableFallThrough()
    module.start()
    
    for data in [
            {"date": "date", "_id": "id", "id": "id", "title": "TESTING"},
            {"date": "date", "_id": "id", "id": "id", "title": "test"},
            {"date": "date", "_id": "id", "id": "id", "title": "testing"},
            {"date": "date", "_id": "id", "id": "id", "title": "some_chars_testing"},
            {"date": "date", "_id": "id", "id": "id", "title": "тест"},
            {"date": "date", "_id": "id", "id": "id", "title": "тестування"},
            {"date": "date", "_id": "id", "id": "id", "title": "[тестування]"},
            ]:
        e = Event(data)
        module.pool.queue.inbox.put(e)
        with pytest.raises(Exception) as e:
            getter(module.pool.queue.outbox)
            assert e.message == 'No event from queue'
