wishbonone-couchdb
==================

Input and Output wishbone module for couchdb

Example Usage
-----

.. code-block:: yaml

   modules:
     input:
       module: wishbone.module.input.couchdbpoller
        arguments:
        couchdb_url: "http://localhost:5984/tenders"
        seqfile: seqfile
  
     fanout:
       module: wishbone.module.flow.fanout

     output_console:
       module: wishbone.module.output.stdout
       arguments:
         colorize: true
          
     output_couchdb:
       module: wishbone.module.output.couchdbpuller
       arguments:
         couchdb_url: "http://localhost:5984/openprocurement"

   routingtable:
   - input.outbox -> fanout.inbox
   - fanout.one   -> output_console.inbox
   - fanout.two   -> output_couchdb.inbox

Installation
------------

pip install 'git+https://github.com/openprocurement/wishbone-couchdb.git#egg=wishbonecouchdb'

Requirements
^^^^^^^^^^^^
See setup.py


Compatibility
-------------

Only python3 is currently supported

Licence
-------

Apache Licence 2.0

Authors
-------

`wishbonone-couchdb` was written by `yshalenyk <yshalenyk@quintagroup.com>`_.
