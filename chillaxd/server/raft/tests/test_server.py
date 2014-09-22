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
import pytest
import six
import zmq

from chillaxd.server.raft import message
from chillaxd.server.raft import peer
from chillaxd.server.raft import server


class TestServer(object):

    def test_init(self):
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

    @mock.patch("chillaxd.server.raft.message.decode_message")
    def test_dispatch_received_message(self, mock_decode_message):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})
        test_server._handle_as_follower = mock.Mock()
        test_server._handle_as_leader = mock.Mock()
        test_server._handle_as_candidate = mock.Mock()
        mock_socket = mock.Mock(spec=zmq.sugar.socket.Socket)
        mock_socket.recv_multipart.return_value = ("server_identifier", "")
        mock_decode_message.return_value = (message.APPEND_ENTRY, 13, "payld")
        test_server._dispatch_received_message(mock_socket, None)
        test_server._handle_as_follower.assert_called_once_with(
            "server_identifier", message.APPEND_ENTRY, 13, "payld")
        assert test_server._handle_as_follower.call_count == 1
        assert test_server._handle_as_candidate.call_count == 0
        assert test_server._handle_as_leader.call_count == 0

        test_server._handle_as_follower.reset_mock()
        test_server._state = server.Server.LEADER
        test_server._dispatch_received_message(mock_socket, None)
        test_server._handle_as_leader.assert_called_once_with(
            "server_identifier", message.APPEND_ENTRY, 13, "payld")
        assert test_server._handle_as_follower.call_count == 0
        assert test_server._handle_as_candidate.call_count == 0
        assert test_server._handle_as_leader.call_count == 1

        test_server._handle_as_leader.reset_mock()
        test_server._state = server.Server.CANDIDATE
        test_server._dispatch_received_message(mock_socket, None)
        test_server._handle_as_candidate.assert_called_once_with(
            "server_identifier", message.APPEND_ENTRY, 13, "payld")
        assert test_server._handle_as_follower.call_count == 0
        assert test_server._handle_as_candidate.call_count == 1
        assert test_server._handle_as_leader.call_count == 0

        test_server._handle_as_candidate.reset_mock()
        test_server._state = -1
        test_server._dispatch_received_message(mock_socket, None)
        assert test_server._handle_as_follower.call_count == 0
        assert test_server._handle_as_candidate.call_count == 0
        assert test_server._handle_as_leader.call_count == 0

    def test_handle_as_leader(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})

        test_server._process_append_entry_request = mock.Mock()
        test_server._process_append_entry_response = mock.Mock()
        test_server._process_request_vote = mock.Mock()

        test_server._handle_as_leader("test_identifier", message.APPEND_ENTRY,
                                      13, "payld")
        test_server._process_append_entry_request.assert_called_once_with(
            "test_identifier", 13, "payld")

        test_server._handle_as_leader("test_identifier",
                                      message.APPEND_ENTRY_RESPONSE,
                                      13, "payld")
        test_server._process_append_entry_response.assert_called_once_with(
            "test_identifier", 13, "payld")

        test_server._handle_as_leader("test_identifier",
                                      message.REQUEST_VOTE,
                                      13, "payld")
        test_server._process_request_vote.assert_called_once_with(
            "test_identifier", 13, "payld")

        test_server._process_append_entry_request.reset_mock()
        test_server._process_append_entry_response.reset_mock()
        test_server._process_request_vote.reset_mock()

        test_server._handle_as_leader("test_identifier",
                                      -1,
                                      13, "payld")

        assert test_server._process_append_entry_request.call_count == 0
        assert test_server._process_append_entry_response.call_count == 0
        assert test_server._process_request_vote.call_count == 0

    def test_handle_as_follower(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})

        test_server._process_append_entry_request = mock.Mock()
        test_server._process_request_vote = mock.Mock()

        test_server._handle_as_follower("test_identifier",
                                        message.APPEND_ENTRY, 13, "payld")
        test_server._process_append_entry_request.assert_called_once_with(
            "test_identifier", 13, "payld")

        test_server._handle_as_follower("test_identifier",
                                        message.REQUEST_VOTE, 13, "payld")
        test_server._process_request_vote.assert_called_once_with(
            "test_identifier", 13, "payld")

        test_server._process_append_entry_request.reset_mock()
        test_server._process_request_vote.reset_mock()

        test_server._handle_as_follower("test_identifier", -1, 13, "payld")

        assert test_server._process_append_entry_request.call_count == 0
        assert test_server._process_request_vote.call_count == 0

    def test_handle_as_candidate(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})

        test_server._process_append_entry_request = mock.Mock()
        test_server._process_request_vote = mock.Mock()
        test_server._process_request_vote_response = mock.Mock()

        test_server._handle_as_candidate("test_identifier",
                                         message.APPEND_ENTRY, 13, "payld")
        test_server._process_append_entry_request.assert_called_once_with(
            "test_identifier", 13, "payld")

        test_server._handle_as_candidate("test_identifier",
                                         message.REQUEST_VOTE, 13, "payld")
        test_server._process_request_vote.assert_called_once_with(
            "test_identifier", 13, "payld")

        test_server._handle_as_candidate("test_identifier",
                                         message.REQUEST_VOTE_RESPONSE,
                                         13, "payld")
        test_server._process_request_vote_response.assert_called_once_with(
            "test_identifier", 13, "payld")

        test_server._process_append_entry_request.reset_mock()
        test_server._process_request_vote.reset_mock()
        test_server._process_request_vote_response.reset_mock()

        test_server._handle_as_candidate("test_identifier", -1, 13, "payld")

        assert test_server._process_append_entry_request.call_count == 0
        assert test_server._process_request_vote.call_count == 0
        assert test_server._process_request_vote_response.call_count == 0

    def test_process_append_entry_request(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})
        test_server._remote_servers = mock.MagicMock()
        test_peer = mock.Mock(spec=peer.Peer)
        test_server._remote_servers.__getitem__.return_value = test_peer

        # stale term
        test_server._process_append_entry_request("test_identifier",
                                                  -1, "payld")
        test_ae_response = message.build_append_entry_response(
            test_server._current_term, False)
        test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(test_ae_response)
        test_server._remote_servers.__getitem__.reset_mock()
        test_peer.reset_mock()

        # current term outdated
        test_server._switch_to_follower = mock.Mock()
        test_server._process_append_entry_request("test_identifier",
                                                  1, "payld")
        test_server._switch_to_follower.assert_called_once_with(
            1, "test_identifier")
        test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_ae_response = message.build_append_entry_response(
            test_server._current_term, False)
        test_peer.send_message.assert_called_once_with(test_ae_response)
        test_server._remote_servers.__getitem__.reset_mock()
        test_peer.reset_mock()
        test_server._switch_to_follower.reset_mock()

        # equals terms as follower
        test_server._process_append_entry_request("test_identifier",
                                                  0, "payld")
        assert test_server._leader == "test_identifier"
        test_ae_response = message.build_append_entry_response(
            test_server._current_term, True)
        test_peer.send_message.assert_called_once_with(test_ae_response)
        test_peer.reset_mock()

        # equals terms as leader
        test_server._state = server.Server.LEADER
        test_server._process_append_entry_request("test_identifier",
                                                  0, "payld")
        assert test_server._leader == "test_identifier"
        test_server._switch_to_follower.assert_called_once_with(
            0, None)
        test_ae_response = message.build_append_entry_response(
            test_server._current_term, True)
        test_peer.send_message.assert_called_once_with(test_ae_response)
        test_peer.reset_mock()
        test_server._switch_to_follower.reset_mock()

        # equals terms as candidate
        test_server._state = server.Server.CANDIDATE
        test_server._process_append_entry_request("test_identifier",
                                                  0, "payld")
        assert test_server._leader == "test_identifier"
        test_server._switch_to_follower.assert_called_once_with(
            0, "test_identifier")
        test_ae_response = message.build_append_entry_response(
            test_server._current_term, True)
        test_peer.send_message.assert_called_once_with(test_ae_response)

    # TODO(yassine)
    def test_process_append_entry_response(self):
        pass

    def test_process_request_vote(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})
        test_server._remote_servers = mock.MagicMock()
        test_peer = mock.Mock(spec=peer.Peer)
        test_server._remote_servers.__getitem__.return_value = test_peer

        # stale term
        test_server._process_request_vote("test_identifier", -1, "payld")
        test_rv_response = message.build_request_vote_response(
            test_server._current_term, False)
        test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(test_rv_response)
        test_server._remote_servers.__getitem__.reset_mock()
        test_peer.reset_mock()

        # current term outdated
        test_server._switch_to_follower = mock.Mock()
        test_server._process_request_vote("test_identifier", 1, "payld")
        test_rv_response = message.build_request_vote_response(
            test_server._current_term, True)
        test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(test_rv_response)
        test_server._remote_servers.__getitem__.reset_mock()
        test_peer.reset_mock()
        test_server._switch_to_follower.reset_mock()

        # equals term, not voted
        test_server._voted_for = None
        test_server._process_request_vote("test_identifier", 0, "payld")
        test_rv_response = message.build_request_vote_response(
            test_server._current_term, True)
        test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(test_rv_response)
        test_server._remote_servers.__getitem__.reset_mock()
        test_peer.reset_mock()
        assert test_server._voted_for == "test_identifier"

        # equals term, voted
        test_server._process_request_vote("test_identifier", 0, "payld")
        test_rv_response = message.build_request_vote_response(
            test_server._current_term, False)
        test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(test_rv_response)

    def test_process_request_vote_response(self):
        test_server = server.Server("127.0.0.1:2406")

        # stale term
        test_server._switch_to_follower = mock.Mock()
        test_server._switch_to_leader = mock.Mock()
        test_server._switch_to_candidate = mock.Mock()
        test_server._process_request_vote_response("test_identifier",
                                                   -1, True)
        assert test_server._switch_to_follower.call_count == 0
        assert test_server._switch_to_follower.call_count == 0
        assert test_server._switch_to_candidate.call_count == 0

        # current term outdated
        test_server._process_request_vote_response("test_identifier",
                                                   1, True)
        test_server._switch_to_follower.assert_called_once_with(1, None)
        test_server._switch_to_follower.reset_mock()

        # equals term with request vote granted with quorum
        assert len(test_server._voters) == 0
        test_server._process_request_vote_response("test_identifier",
                                                   0, True)
        assert test_server._voters == {"test_identifier"}
        test_server._switch_to_leader.assert_called_once_with()
        test_server._switch_to_leader.reset_mock()

        # equals term with request vote denied
        test_server._process_request_vote_response("test_identifier",
                                                   0, False)
        assert test_server._switch_to_follower.call_count == 0
        assert test_server._switch_to_follower.call_count == 0
        assert test_server._switch_to_candidate.call_count == 0

    def test_send_heartbeat(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})
        pytest.raises(server.InvalidState, test_server._send_heartbeat)

        test_server._state = server.Server.LEADER
        test_server._broadcast_message = mock.Mock()
        test_server._send_heartbeat()
        test_heartbeat = message.build_append_entry(test_server._current_term,
                                                    None)
        test_server._broadcast_message.assert_called_once_with(test_heartbeat)

    def test_election_timeout_task(self):
        test_server = server.Server("127.0.0.1:2406")

        test_server._state = server.Server.LEADER
        pytest.raises(server.InvalidState, test_server._election_timeout_task)

        # with one node
        test_server._state = server.Server.FOLLOWER
        test_server._switch_to_leader = mock.Mock()
        test_server._switch_to_candidate = mock.Mock()
        test_server._election_timeout_task()
        test_server._switch_to_leader.assert_called_once_with()
        assert test_server._switch_to_candidate.call_count == 0

        # with several nodes
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})
        test_server._state = server.Server.FOLLOWER
        test_server._switch_to_leader = mock.Mock()
        test_server._switch_to_candidate = mock.Mock()
        test_server._election_timeout_task()
        test_server._switch_to_candidate.assert_called_once_with()
        assert test_server._switch_to_leader.call_count == 0
        assert test_server._leader is None

    def test_broadcast_message(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})
        test_peer_1 = mock.Mock(spec=peer.Peer)
        test_peer_2 = mock.Mock(spec=peer.Peer)
        test_server._remote_servers["127.0.0.1:2407"] = test_peer_1
        test_server._remote_servers["127.0.0.1:2408"] = test_peer_2

        test_server._broadcast_message(b"test_message")
        test_peer_1.send_message.assert_called_once_with(b"test_message")
        test_peer_2.send_message.assert_called_once_with(b"test_message")

    def test_switch_to_leader(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})

        pytest.raises(server.InvalidState, test_server._switch_to_leader)

        test_server._state = server.Server.CANDIDATE

        test_server._send_heartbeat = mock.Mock()
        test_server._heartbeating = mock.Mock()
        test_server._check_leader_timeout = mock.Mock()

        test_server._switch_to_leader()

        assert test_server._state == server.Server.LEADER
        assert len(test_server._voters) == 0
        assert test_server._voted_for is None

        test_server._send_heartbeat.assert_called_once_with()
        test_server._send_heartbeat.reset_mock()
        test_server._heartbeating.start.assert_called_once_with()
        test_server._heartbeating.reset_mock()
        test_server._check_leader_timeout.stop.assert_called_once_with()
        test_server._check_leader_timeout.reset_mock()

        test_server._remote_server_endpoints = {}
        test_server._state = server.Server.CANDIDATE
        test_server._switch_to_leader()
        assert test_server._send_heartbeat.call_count == 0
        assert test_server._heartbeating.start.call_count == 0
        test_server._check_leader_timeout.stop.assert_called_once_with()

    def test_switch_to_follower(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})

        test_server._state = server.Server.LEADER

        test_server._heartbeating = mock.Mock()
        test_server._check_leader_timeout = mock.Mock()

        test_server._switch_to_follower(1, "new_leader")

        assert test_server._state == server.Server.FOLLOWER
        assert test_server._leader == "new_leader"
        assert test_server._current_term == 1
        assert len(test_server._voters) == 0
        assert test_server._voted_for is None

        test_server._heartbeating.stop.assert_called_once_with()
        test_server._check_leader_timeout.start.assert_called_once_with()

    def test_switch_to_candidate(self):
        test_server = server.Server("127.0.0.1:2406", {"127.0.0.1:2407",
                                                       "127.0.0.1:2408"})

        test_server._state = -1
        pytest.raises(server.InvalidState, test_server._switch_to_candidate)

        test_server._state = server.Server.FOLLOWER
        test_server._broadcast_message = mock.Mock()
        test_server._check_leader_timeout = mock.Mock()

        test_server._switch_to_candidate()

        assert test_server._voters == {test_server._local_server_endpoint}
        assert test_server._state == server.Server.CANDIDATE
        assert test_server._voted_for == test_server._local_server_endpoint
        rv_message = message.build_request_vote(test_server._current_term,
                                                None)
        test_server._broadcast_message.assert_called_once_with(rv_message)
        assert test_server._check_leader_timeout.callback_time != 0
