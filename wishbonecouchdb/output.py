from couchdb import Database
from ujson import loads
from wishbone.module import OutputModule
from gevent import spawn
from uuid import uuid4


class CouchdbPush(OutputModule):

    def __init__(
            self,
            actor_config,
            couchdb_url,
            payload=None,
            selection="data",
            bulk=100,
            parallel_streams=1,
            native_events=False,
            **kw
            ):
        OutputModule.__init__(self, actor_config)
        self.pool.createQueue("inbox")
        self.registerConsumer(self.consume, "inbox")
        self.couchdb = Database(couchdb_url)
        self._bulk_size = bulk
        self._bulk = {}


    def __save(self):
        self.logging.debug(
            "Saving: {} docs".format(len(self._bulk))
        )
        try:
            responce = self.couchdb.update([
                doc for doc in self._bulk.values()
            ])
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
        finally:
            self._bulk = {}
            self.logging.debug("Cleaned bulk")

        return False

    def consume(self, event):
        data = self.encode(
            self.getDataToSubmit(
                event
            )
        )
        if not isinstance(data, dict):
            try:
                data = loads(data)
            except ValueError:
                self.logging.error(
                    "Unable to parse data from raw string. Skipping"
                )
        id = data.get('id', data.get('_id'))
        if id:
            data['_id'] = data['id'] = id 
        if id and (id in self.couchdb):
            rev = self.couchdb.get(id).rev
            data['_rev'] = rev
            self.logging.debug("Update revision in data {} to {}".format(
                id,
                rev
            ))
        self._bulk[data.get('_id', uuid4().hex)] = data
        self.logging.debug("Added {} to bulk queue. Size {}".format(id, len(self._bulk)))
        if len(self._bulk) >= self._bulk_size:
            g = spawn(self.__save)
            g.join()
