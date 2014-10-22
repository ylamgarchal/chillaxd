# Chillaxd [![Build Status](https://api.travis-ci.org/ylamgarchal/chillaxd.png)](https://api.travis-ci.org/ylamgarchal/chillaxd)

Chillaxd is a Python distributed coordination framework based on ZeroMQ and RAFT consensus algorithm.

For more details on RAFT, you can read [In Search of an Understandable Consensus Algorithm][raft-paper] by Diego Ongaro and John Ousterhout, Stanford University.

## Status of the RAFT implementation

* Leader election
* ~~Log replication~~
* ~~Cluster membership changes~~
* ~~Log compaction~~
* ~~Client interactions with the state machine~~

## Install dependencies
```
$ sudo apt-get install python-pip python-dev python3.4-dev -y
```

## Install Chillaxd
```
$ git clone https://github.com/ylamgarchal/chillaxd.git
```

```
$ cd chillaxd
```

```
$ sudo python setup.py install
```

## Running tests

```
$ cd chillaxd
```

```
$ tox
```

[raft-paper]: https://ramcloud.stanford.edu/raft.pdf
