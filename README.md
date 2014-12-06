# Chillaxd [![Build Status](https://api.travis-ci.org/ylamgarchal/chillaxd.png)](https://api.travis-ci.org/ylamgarchal/chillaxd)

Chillaxd is a Python distributed coordination framework based on ZeroMQ and RAFT consensus algorithm.

For more details on RAFT, you can read [In Search of an Understandable Consensus Algorithm][raft-paper] by Diego Ongaro and John Ousterhout, Stanford University.

## Status and priority of the implementation

* Leader election
* In memory log replication
* ~~Client interactions with the state machine~~
* ~~Log persistence~~
* ~~Log compaction~~
* ~~Cluster membership changes~~


## Install dependencies
```
$ sudo apt-get install python-pip python-dev python3.4-dev -y
```

## Install Chillaxd
```sh
$ git clone https://github.com/ylamgarchal/chillaxd.git
$ cd chillaxd
$ sudo python setup.py install
```

## Running tests

```sh
$ cd chillaxd
$ tox
```

## Running three node cluster on the same machine

First, open four consoles, the three first console will print the servers logs
while the fourth is used to run client commands.

```sh
$ chillaxd-server 1234 1235 1236
```

```sh
$ chillaxd-server 1235 1234 1236
```

```sh
$ chillaxd-server 1236 1234 1235
```

Now the cluster is up and running. The logs indicates which one is the current
leader. Let's try to send some commands.

For instance if the current leader is the one listening on port 1234.

```sh
$ chillaxd-client 1234 create_node /node_1  data_1
```

We created a node on / named "node_1" associated to the data "data_1", we can
then verify that this commands is replicated on each server.

```sh
$ chillaxd-client 1234 get_children /
['node_1']
```

```sh
$ chillaxd-client 1235 get_children /
['node_1']
```

```sh
$ chillaxd-client 1236 get_children /
['node_1']
```

For using the other available commands, please see the Python module chillaxd.commands.py

[raft-paper]: https://ramcloud.stanford.edu/raft.pdf
