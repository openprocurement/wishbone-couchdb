import os.path
from ujson import loads
from couchdb import Database
from couchdb.http import HTTPError
from gevent import sleep
from wishbone.module import InputModule


class CouchdbPoller(InputModule):

    def __init__(
            self,
            actor_config,
            couchdb_url,
            native_events=False,
            seqfile="seqfile",
            destination="data",
            since=0,
            **kw
            ):
        InputModule.__init__(self, actor_config)
        self.pool.createQueue("outbox")
        self.since = since
        self.seqfile = seqfile
        self.kw = kw
        try:
            self.couchdb = Database(couchdb_url)
        except HTTPError:
            self.logging.fatal("Invalid database name")
            # TODO: create db

    def __get_doc(self, doc_id):
        return loads(self.couchdb.resource.get(doc_id)[2].read())

    def preHook(self):
        if os.path.exists(self.seqfile):
            with open(self.seqfile) as seqfile:
                self.since = seqfile.read()
                self.logging.info('Restoring from seq: {}'.format(self.since))
        self.sendToBackground(self.produce)

    def postHook(self):
        with open(self.seqfile, 'w+') as seqfile:
            seqfile.write(str(self.since))

    def produce(self):
        while self.loop():
            for feed in self.couchdb.changes(feed="continuous", since=self.since):
                self.since = feed.get('seq', feed.get('last_seq', "now"))
                self.logging.info("Change event {}".format(feed))
                if 'id' in feed:
                    doc = self.__get_doc(feed['id'])
                    event = self.generateEvent(
                        doc,
                        self.kwargs.destination
                    )
                    self.submit(event, "outbox")
                sleep(0)
        self.logging.info("Stopping changes feed from couchdb")
