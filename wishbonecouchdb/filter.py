import operator
import jq
from couchdb import Database
from wishbone.module import FlowModule
from wishbone.event import extractBulkItems, Event
from wishbone.error import ModuleInitFailure


class ExpressionMixin:
    def prepare_expressions(self):
        valid = []
        for condition in self.kwargs.conditions:
            try:
                condition['compiled'] = jq.jq(condition['expression'])
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
            result = condition['compiled'].transform(data)
            if result:
                matched = True
                queue = condition['queue']
                if queue == 'no_match':
                    self.logging.warn("{}: skipped by filter: {}".format(
                        data.get('id', ''), condition['name'])
                    )
                    continue
                self.submit(event, queue)


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
        self.view_expression = jq.jq(view_expression)

    def consume(self, event):
        self.logging.debug("Event from inbox {}".format(event))
        data = event.get(self.kwargs.selection)

        try:
            resp = self.couchdb.view(
                self.kwargs.view,
                key=self.view_expression.transform(data)
                )
            view_value = next(iter(resp.rows), False)
            if view_value:
                for expression in self.conditions:
                    result = expression['compiled'].transform([view_value, data])
                    if result:
                        self.logging.debug("Expression {} matches data {}".format(
                            expression['expression'], [view_value, data]
                        ))
                        queue = expression.get('queue', 'outbox')
                        self.submit(event, queue)
                    else:
                        continue
            else:
                self.submit(event, 'outbox')
        except Exception as e:
            self.logging.error("Error on view filter {}".format(e))


class DateModifiedFilter(FlowModule):

    def __init__(
        self,
        actor_config,
        couchdb_url,
        view="releases/datemodified_filter",
        selection="data"
    ):
        FlowModule.__init__(self, actor_config)
        self.couchdb = Database(couchdb_url)
        for q in ('inbox', 'outbox'):
            self.pool.createQueue(q)
        self.registerConsumer(self.consume, 'inbox')

    def consume(self, event):
        self.logging.debug("Event from inbox {}".format(event))

        if event.isBulk():
            bulk_docs = {}

            try:
                for e in extractBulkItems(event):
                    doc = e.get(self.kwargs.selection)
                    doc_id = doc.pop('id', doc.pop('_id', ''))
                    if doc_id:
                        doc['_id'] = doc['id'] = doc_id
                    bulk_docs[doc['id']] = doc

                bulk_docs_to_check, checked_bulk_docs = list(bulk_docs.keys()), list()
                # updated docs
                for row in self.couchdb.view(self.kwargs.view, keys=bulk_docs_to_check).rows:
                    if row.id in bulk_docs:
                        checked_bulk_docs.append(row.id)
                        if (bulk_docs[row.id]['date'] > row['value']):
                            self.logging.debug(
                                "datemodified_filter {} on doc {} higher then value {}".format(
                                    bulk_docs[row.id]['date'],
                                    row.id,
                                    row['value']
                                )
                            )
                            self.submit(Event(bulk_docs[row.id]), 'outbox')
                # new docs
                for doc_id in list(set(bulk_docs_to_check) - set(checked_bulk_docs)):
                    self.logging.debug(
                        "datemodified_filter {} on doc {} new doc".format(
                            bulk_docs[doc_id]['date'],
                            doc_id
                        )
                    )
                    self.submit(Event(bulk_docs[doc_id]), 'outbox')

            except Exception as e:
                self.logging.error("Error on datemodified_filter {}".format(e))

        else:
            data = event.get(self.kwargs.selection)

            try:
                resp = self.couchdb.view(
                    self.kwargs.view,
                    key=data.id
                ).rows
                if not resp:
                    self.submit(event, 'outbox')
                    return

                row = next(iter(resp), False)
                if row:
                    if (data['date'] > row['value']):
                        self.logging.debug(
                            "datemodified_filter {} on doc {} higher then value {}".format(
                                data['date'],
                                row.id,
                                row['value']
                            )
                        )
                        self.submit(event, 'outbox')
                else:
                    self.submit(event, 'outbox')

            except Exception as e:
                self.logging.error("Error on datemodified_filter {}".format(e))
