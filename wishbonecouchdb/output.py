from couchdb import Database
from ujson import loads
from wishbone.module import OutputModule


class CouchdbPuller(OutputModule):

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

    def __save(self, data, id=None):
        self.logging.debug(
            "Saving: {}".format(data)
        )
        try:
            return self.couchdb.save(data)
        except Exception as e:
            self.logging.error(
                "Error {} on save data {}".format(
                    e,
                    id
                )
            )
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
                    "Unable to parse data from raw string"
                )
                self.logging.info('Will attempt to save as is')
                doc, _rev = self.__save(data, None)
                if doc:
                    self.logging.info("Saved {}".format(doc))
                return
        id = data.get('id', data.get('_id'))
        if id and (id in self.couchdb):
            rev = self.couchdb.get(id).rev
            data['_rev'] = rev
            self.logging.info("Update revision in data {} to {}".format(
                id,
                rev
            ))
        data['_id'] = id
        doc, _rev = self.__save(data, id)
        if doc:
            self.logging.info("Saved {}".format(doc))
