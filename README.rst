Chillaxd
========

.. image:: https://api.travis-ci.org/ylamgarchal/chillaxd.png
    :target: https://api.travis-ci.org/ylamgarchal/chillaxd

Chillaxd is a Python distributed coordination framework based on ZeroMQ and RAFT consensus algorithm.

For more details on RAFT, you can read `In Search of an Understandable Consensus Algorithm <https://ramcloud.stanford.edu/raft.pdf>`_ by Diego Ongaro and John Ousterhout, Stanford University.

Status and priority of the implementation
-----------------------------------------

* Leader election [DONE]
* In memory log replication [DONE]
* Client interactions with the state machine [IN PROGRESS]
* Log persistence [TODO]
* Log compaction [TODO]
* Cluster membership changes [TODO]


Install dependencies
--------------------

 $ sudo apt-get install python-pip python-dev python3.4-dev -y

Install Chillaxd from Pypi
--------------------------

 $ pip install chillaxd

Install Chillaxd from source
----------------------------

 $ git clone https://github.com/ylamgarchal/chillaxd.git

 $ cd chillaxd

 $ sudo python setup.py install


Running tests
-------------

 $ cd chillaxd

 $ tox


Running three node cluster on the same machine
----------------------------------------------

First, open four consoles, the three first console will print the servers logs
while the fourth is used to run client commands.


 $ chillaxd --config-file ./chillaxd_1.conf

 2014-12-28 18:44:19,252 :: INFO :: let's chillax on '127.0.0.1:27001'...



 $ chillaxd --config-file ./chillaxd_2.conf

 2014-12-28 18:44:19,252 :: INFO :: let's chillax on '127.0.0.1:27002'...



 $ chillaxd --config-file ./chillaxd_3.conf

 2014-12-28 18:44:19,252 :: INFO :: let's chillax on '127.0.0.1:27003'...


Now the cluster is up and running. The logs indicates which one is the current
leader. Let's try to send some commands.

For instance if the current leader is the one listening on port 27001.

**WARNING: for now, it's a minimalist client for demonstrating the replication mechanism.**


 $ export CHILLAXD_SERVER=127.0.0.1:27001

 $ chillax create_node /node_1  data_1
 
 ACK command 'c154482f-1ba7-4d0c-b8a4-b54d3807e2a2'


We created a node on the root "/" named "node_1" associated to the data "data_1", we
can then verify that this commands is replicated on each server.


 $ export CHILLAXD_SERVER=127.0.0.1:27001

 $ chillax 127.0.0.1:27001 get_children /
 
 ['node_1']

 $ export CHILLAXD_SERVER=127.0.0.1:27002

 $ chillax 127.0.0.1:27002 get_children /
 
 ['node_1']

 $ export CHILLAXD_SERVER=127.0.0.1:27003

 $ chillax 127.0.0.1:27003 get_children /
 
 ['node_1']
