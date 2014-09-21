# -*- coding: utf-8 -*-
# Author: Yassine Lamgarchal <lamgarchal.yassine@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import mock
import six
import zmq

import chillaxd.server.raft.peer as peer
from chillaxd.server.raft import server


class TestServer(object):

    def test_instantiation(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})

        assert test_server._local_server_endpoint == "127.0.0.1:2406"
        assert test_server._remote_server_endpoints == {"127.0.0.1:2407",
                                                        "127.0.0.1:2408"}
        assert test_server._remote_servers == {}
        assert test_server._quorum == 2
        assert test_server._check_leader_timeout is None
        assert test_server._heartbeating is None
        assert test_server._state == server.Server.FOLLOWER
        assert test_server._leader is None
        assert test_server._voters == set()
        assert test_server._voted_for is None
        assert test_server._log == []
        assert test_server._current_term == 0
        assert test_server._commit_index == 0
        assert test_server._last_applied == 0
        assert test_server._context is None
        assert test_server._zmq_ioloop is None
        assert test_server._zmq_router is None
        assert test_server._is_started is False

    @mock.patch("chillaxd.server.raft.server.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.server.raft.server.zmq.Context", spec=zmq.Context)
    def test_setup(self, zmq_context, zmq_ioloop):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})
        test_server._setup()

        zmq_context.assert_called_once_with()

        for remote_server in six.itervalues(test_server._remote_servers):
            assert isinstance(remote_server, peer.Peer)
        assert len(test_server._remote_servers) == 2

        ioloop_instance = zmq_ioloop().instance
        ioloop_instance.assert_called_once_with()
        ioloop_instance().add_handler.assert_called_once_with(
            test_server._zmq_router,
            test_server._dispatch_received_message,
            zmq.POLLIN)

        assert isinstance(test_server._check_leader_timeout,
                          zmq.eventloop.ioloop.PeriodicCallback)

        assert (test_server._check_leader_timeout.callback ==
                test_server._election_timeout_task)

        assert (test_server._heartbeating.callback ==
                test_server._send_heartbeat)

    @mock.patch(
        "chillaxd.server.raft.server.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.server.raft.server.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.server.raft.server.zmq.Context", spec=zmq.Context)
    def test_start(self, zmq_context, zmq_ioloop, zmq_periodic_callback):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})
        test_server.start()
        zmq_context().socket().bind.assert_called_once_with("tcp://%s" %
                                                            "127.0.0.1:2406")

        for remote_server in six.itervalues(test_server._remote_servers):
                assert remote_server._is_started is True
        assert test_server._is_started is True
        zmq_ioloop().instance().start.assert_called_once_with()

    @mock.patch(
        "chillaxd.server.raft.server.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.server.raft.server.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.server.raft.server.zmq.Context", spec=zmq.Context)
    def test_stop(self, zmq_context, zmq_ioloop, zmq_periodic_callback):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})
        test_server.start()
        test_server.stop()

        for remote_server in six.itervalues(test_server._remote_servers):
                assert remote_server._is_started is False

        zmq_context().destroy.assert_called_once_with(linger=0)
        zmq_ioloop().instance().stop.assert_called_once_with()
        assert test_server._is_started is False

    @mock.patch(
        "chillaxd.server.raft.server.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.server.raft.server.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.server.raft.server.zmq.Context", spec=zmq.Context)
    def test_handle_signals(self, zmq_context, zmq_ioloop,
                            zmq_periodic_callback):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})
        test_server.start()
        assert test_server._is_started is True
        test_server._handle_signals(None, None)
        assert test_server._is_started is False
