import operator
import jq
from couchdb import Database
from wishbone.module import FlowModule


class CouchdbFilter(FlowModule):

    def __init__(
        self,
        actor_config,
        couchdb_url,
        view_path,
        filter_key,
        filter_value,
        op,
    ):
        FlowModule.__init__(self, actor_config)
        self.pool.createQueue("outbox")
        self.view_path = view_path
        self.filter_key = jq.jq(filter_key)
        self.filter_value = jq.jq(filter_value)
        self.op = getattr(operator, op)
        self.pool.createQueue('outbox')
        self.pool.createQueue('inbox')
        self.couchdb = Database(couchdb_url)
        self.registerConsumer(self.consume, 'inbox')

    def consume(self, event):
        self.logging.debug("Event from inbox {}".format(event))

        data = event.dump().get('data', {})
        resp = self.couchdb.view(
            self.view_path,
            key=self.filter_key.transform(data)
        )
        if resp.rows:
            old = resp.rows[0].value
            new = self.filter_value.transform(data)
            if not self.op(old, new):
                self.logging.debug('Skipped {} by filter. Op: {} old: {} new {}'.format(
                    data['id'],
                    self.op.__name__,
                    old,
                    new
                    ))
                del data
                return
        self.submit(event, 'outbox')
            




            
        
        
        
