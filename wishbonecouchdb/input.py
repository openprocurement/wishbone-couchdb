""" __init__.py - Wishbone couchdb input module """
import os.path
from couchdb import Database
from couchdb.http import HTTPError
from wishbone.module import InputModule
from wishbone.event import Event


class CouchdbPoller(InputModule):

    def __init__(
            self,
            actor_config,
            couchdb_url,
            native_events=False,
            seqfile="seqfile",
            destination="data",
            since=0,
            ):
        InputModule.__init__(self, actor_config)
        self.pool.createQueue("outbox")
        self.since = since
        self.seqfile = seqfile
        try:
            self.couchdb = Database(couchdb_url)
        except HTTPError:
            self.logging.error("Invalid database name")
            # TODO: create db

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
            for feed in self.couchdb.changes(
                    feed="continuous", since=self.since, include_docs=True
            ):
                self.since = feed.get('seq', feed.get('last_seq', "now"))
                self.logging.debug("Change event {}".format(feed))
                if feed and 'doc' in feed:
                    self.submit(Event(feed['doc']), "outbox")
        self.logging.info("Stopping changes feed from couchdb")
