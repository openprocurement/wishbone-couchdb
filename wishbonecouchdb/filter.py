
import operator
import pyjq
from couchdb import Database
from wishbone.module import FlowModule
from wishbone.error import ModuleInitFailure


class ExpressionMixin:
    def prepare_expressions(self):
        valid = []
        for condition in self.kwargs.conditions:
            try:
                condition['compiled'] = pyjq.compile(condition['expression'])
                valid.append(condition)
                q = condition.get('queue', 'outbox')
                if not self.pool.hasQueue(q):
                    self.pool.createQueue(q)
            except Exception:
                self.logging.error("{}: invalid jq expression {}".format(
                    condition['name'], condition['expression']
                    ))
        self.conditions = valid


class JQFilter(FlowModule, ExpressionMixin):
    """ Mostly based on wishbone-flow-jq module """


    def __init__(self, actor_config, selection="data", conditions=[]):
        FlowModule.__init__(self, actor_config)
        self.pool.createQueue('inbox')

        self.registerConsumer(self.consume, 'inbox')
        self.prepare_expressions()
        if not self.pool.hasQueue("outbox"):
            self.pool.createQueue("outbox")

    def consume(self, event):
        self.logging.debug("Event from inbox {}".format(event))
        data = event.get(self.kwargs.selection)
        matched = False
        for condition in self.conditions:
            result = condition['compiled'].first(data)
            if result:
                matched = True
                queue = condition['queue']
                if queue == 'no_match':
                    self.logging.warn("{}: skipped by filter: {}".format(
                        data.get('id', ''), condition['name'])
                    )
                    continue
                self.submit(event, queue)
        if not matched:
            self.submit(event, "outbox")


class ViewFilter(FlowModule, ExpressionMixin):

    def __init__(
        self,
        actor_config,
        couchdb_url,
        view,
        view_expression,
        conditions=[],
        selection="data"
    ):
        FlowModule.__init__(self, actor_config)
        self.couchdb = Database(couchdb_url)
        self.pool.createQueue('inbox')
        self.registerConsumer(self.consume, 'inbox')
        self.prepare_expressions()
        self.view_expression = pyjq.compile(view_expression)

    def consume(self, event):
        self.logging.debug("Event from inbox {}".format(event))
        data = event.get(self.kwargs.selection)

        resp = self.couchdb.view(
            self.kwargs.view,
            key=self.view_expression.first(data)
            )
        view_value = next(iter(resp.rows), False)
        if view_value:
            for expression in self.conditions:
                result = expression['compiled'].first([view_value, data])
                if result:
                    self.logging.debug("Expression {} matches data {}".format(
                        expression['expression'], [view_value, data]
                    ))
                    queue = expression.get('queue', 'outbox')
                    self.submit(data, queue)
                else:
                    del data
