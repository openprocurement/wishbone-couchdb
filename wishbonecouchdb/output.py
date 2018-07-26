from uuid import uuid4
from couchdb import Database
from ujson import loads
from gevent import spawn
from wishbone.module import OutputModule
from wishbone.event import extractBulkItems


class CouchdbOutput(OutputModule):

    def __init__(
            self,
            actor_config,
            couchdb_url,
            payload=None,
            selection="data",
            parallel_streams=1,
            native_events=False,
            **kw
            ):
        OutputModule.__init__(self, actor_config)
        self.pool.createQueue("inbox")
        self.registerConsumer(self.consume, "inbox")
        self.couchdb = Database(couchdb_url)

    def consume(self, event):
        if event.isBulk():
            bulk_docs = {}
            for e in extractBulkItems(event):
                doc = e.get(self.kwargs.selection)
                doc_id = doc.pop('id', doc.pop('_id', ''))
                if doc_id:
                    doc['_id'] = doc['id'] = doc_id
                bulk_docs[doc['id']] = doc

            for row in self.couchdb.view(
                    '_all_docs',
                    keys=list(bulk_docs.keys())).rows:
                if row.id in bulk_docs:
                    bulk_docs[row.id]['_rev'] = row['value']['rev']
            try:
                responce = self.couchdb.update(list(bulk_docs.values()))
                for ok, doc_id, rest in responce:
                    if ok:
                        self.logging.info(
                            "Saved {}".format(doc_id)
                        )
                    else:
                        self.logging.error(
                            "Error on save bulk. Type {}, message {}, doc {}".format(
                                rest,
                                getattr(rest, 'message', ''),
                                doc_id
                            )
                        )
            except Exception as e:
                self.logging.error(
                    "Uncaught error {} on save bulk".format(
                        e,
                    )
                )
        else:
            data = event.get(self.kwargs.selection)
            doc_id = data.get('id', data.get('_id'))
            if doc_id:
                data['_id'] = data['id'] = doc_id
                if doc_id in self.couchdb:
                    rev = self.couchdb.get(id).rev
                    data['_rev'] = rev
                    self.logging.debug("Update revision in data {} to {}".format(
                        id,
                        rev
                    ))
            self.couchdb.save(data)
