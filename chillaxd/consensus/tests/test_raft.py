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
from chillaxd.consensus import log
from chillaxd.consensus import peer
from chillaxd.consensus import raft

from chillaxd.consensus import message


class TestServer(object):

    def setup_method(self, method):
        self.test_server = raft.Raft("127.0.0.1:2406", {"127.0.0.1:2407",
                                                        "127.0.0.1:2408"})

    def test_init(self):

        assert self.test_server._local_server_endpoint == "127.0.0.1:2406"
        assert self.test_server._remote_server_endpoints == {"127.0.0.1:2407",
                                                             "127.0.0.1:2408"}
        assert self.test_server._remote_servers == {}
        assert self.test_server._quorum == 2
        assert self.test_server._check_leader_timeout is None
        assert self.test_server._heartbeating is None
        assert self.test_server._state == raft.Raft.FOLLOWER
        assert self.test_server._leader is None
        assert self.test_server._voters == set()
        assert self.test_server._voted_for is None
        assert isinstance(self.test_server._log, log.RaftLog)
        assert self.test_server._current_term == 0
        assert self.test_server._commit_index == 0
        assert self.test_server._last_applied == 0
        assert self.test_server._context is None
        assert self.test_server._zmq_ioloop is None
        assert self.test_server._socket_for_consensus is None
        assert self.test_server._is_started is False

    @mock.patch("chillaxd.consensus.raft.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.consensus.raft.zmq.Context", spec=zmq.Context)
    def test_setup(self, zmq_context, zmq_ioloop):

        self.test_server._setup()
        zmq_context.assert_called_once_with()

        for remote_server in six.itervalues(self.test_server._remote_servers):
            assert isinstance(remote_server, peer.Peer)
        assert len(self.test_server._remote_servers) == 2

        ioloop_instance = zmq_ioloop().instance
        ioloop_instance.assert_called_once_with()
        assert ioloop_instance().add_handler.call_count == 2

        assert isinstance(self.test_server._check_leader_timeout,
                          zmq.eventloop.ioloop.PeriodicCallback)

        assert (self.test_server._check_leader_timeout.callback ==
                self.test_server._election_timeout_task)

        assert (self.test_server._heartbeating.callback ==
                self.test_server._send_heartbeat)

    @mock.patch(
        "chillaxd.consensus.raft.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.consensus.raft.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.consensus.raft.zmq.Context", spec=zmq.Context)
    def test_start(self, zmq_context, zmq_ioloop, zmq_periodic_callback):
        self.test_server.start()
        assert zmq_context().socket().bind.call_count == 2

        for remote_server in six.itervalues(self.test_server._remote_servers):
                assert remote_server._is_started is True
        assert self.test_server._is_started is True
        zmq_ioloop().instance().start.assert_called_once_with()

    @mock.patch(
        "chillaxd.consensus.raft.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.consensus.raft.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.consensus.raft.zmq.Context", spec=zmq.Context)
    def test_stop(self, zmq_context, zmq_ioloop, zmq_periodic_callback):
        self.test_server.start()
        self.test_server.stop()

        for remote_server in six.itervalues(self.test_server._remote_servers):
                assert remote_server._is_started is False

        zmq_context().destroy.assert_called_once_with(linger=0)
        zmq_ioloop().instance().stop.assert_called_once_with()
        assert self.test_server._is_started is False

    @mock.patch(
        "chillaxd.consensus.raft.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.consensus.raft.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.consensus.raft.zmq.Context", spec=zmq.Context)
    def test_handle_signals(self, zmq_context, zmq_ioloop,
                            zmq_periodic_callback):
        self.test_server.start()
        assert self.test_server._is_started is True
        self.test_server._handle_signals(None, None)
        assert self.test_server._is_started is False

    def test_dispatch_internal_raft_message(self):

        self.test_server._handle_as_follower = mock.Mock()
        self.test_server._handle_as_leader = mock.Mock()
        self.test_server._handle_as_candidate = mock.Mock()
        mock_socket = mock.Mock(spec=zmq.sugar.socket.Socket)
        mock_socket.recv_multipart.return_value = ("server_identifier",
                                                   "payload")

        self.test_server._dispatch_internal_raft_message(mock_socket,
                                                         zmq.POLLIN)
        self.test_server._handle_as_follower.assert_called_once_with(
            "server_identifier", "payload")
        assert self.test_server._handle_as_follower.call_count == 1
        assert self.test_server._handle_as_candidate.call_count == 0
        assert self.test_server._handle_as_leader.call_count == 0

        self.test_server._handle_as_follower.reset_mock()
        self.test_server._state = raft.Raft.LEADER
        self.test_server._dispatch_internal_raft_message(mock_socket,
                                                         zmq.POLLIN)
        self.test_server._handle_as_leader.assert_called_once_with(
            "server_identifier", "payload")
        assert self.test_server._handle_as_follower.call_count == 0
        assert self.test_server._handle_as_candidate.call_count == 0
        assert self.test_server._handle_as_leader.call_count == 1

        self.test_server._handle_as_leader.reset_mock()
        self.test_server._state = raft.Raft.CANDIDATE
        self.test_server._dispatch_internal_raft_message(mock_socket,
                                                         zmq.POLLIN)
        self.test_server._handle_as_candidate.assert_called_once_with(
            "server_identifier", "payload")
        assert self.test_server._handle_as_follower.call_count == 0
        assert self.test_server._handle_as_candidate.call_count == 1
        assert self.test_server._handle_as_leader.call_count == 0

        self.test_server._handle_as_candidate.reset_mock()
        self.test_server._state = -1
        self.test_server._dispatch_internal_raft_message(mock_socket,
                                                         zmq.POLLIN)
        assert self.test_server._handle_as_follower.call_count == 0
        assert self.test_server._handle_as_candidate.call_count == 0
        assert self.test_server._handle_as_leader.call_count == 0

    def test_handle_as_leader(self):

        self.test_server._process_append_entry_request = mock.Mock()
        self.test_server._process_append_entry_response = mock.Mock()
        self.test_server._process_request_vote = mock.Mock()
        self.test_server._process_request_vote_response = mock.Mock()

        aereq_message = (1, 2, 3, 4, ())
        aer_packed = message.build_append_entry_request(*aereq_message)

        self.test_server._handle_as_leader("test_identifier", aer_packed)
        self.test_server._process_append_entry_request.assert_called_once_with(
            "test_identifier", *aereq_message)

        aeresp = (1, True)
        aeresp_packed = message.build_append_entry_response(*aeresp)
        self.test_server._handle_as_leader("test_identifier", aeresp_packed)
        self.test_server._process_append_entry_response.\
            assert_called_once_with("test_identifier", *aeresp)

        rv = (1, 2, 3)
        rv_packed = message.build_request_vote(*rv)
        self.test_server._handle_as_leader("test_identifier", rv_packed)
        self.test_server._process_request_vote.assert_called_once_with(
            "test_identifier", *rv)

    def test_handle_as_follower(self):

        self.test_server._process_append_entry_request = mock.Mock()
        self.test_server._process_request_vote = mock.Mock()

        aereq_message = (1, 2, 3, 4, ())
        aer_packed = message.build_append_entry_request(*aereq_message)
        self.test_server._handle_as_follower("test_identifier", aer_packed)
        self.test_server._process_append_entry_request.assert_called_once_with(
            "test_identifier", *aereq_message)

        rv_message = (1, 2, 3)
        rv_packed = message.build_request_vote(*rv_message)
        self.test_server._handle_as_follower("test_identifier", rv_packed)
        self.test_server._process_request_vote.assert_called_once_with(
            "test_identifier", *rv_message)

        self.test_server._process_append_entry_request.reset_mock()
        self.test_server._process_request_vote.reset_mock()

    def test_handle_as_candidate(self):

        self.test_server._process_append_entry_request = mock.Mock()
        self.test_server._process_request_vote = mock.Mock()
        self.test_server._process_request_vote_response = mock.Mock()

        ae_req_message = (1, 2, 3, 4, ())
        aer_packed = message.build_append_entry_request(*ae_req_message)
        self.test_server._handle_as_candidate("test_identifier", aer_packed)
        self.test_server._process_append_entry_request.assert_called_once_with(
            "test_identifier", *ae_req_message)

        rv_message = (1, 2, 3)
        rv_packed = message.build_request_vote(*rv_message)
        self.test_server._handle_as_candidate("test_identifier", rv_packed)
        self.test_server._process_request_vote.assert_called_once_with(
            "test_identifier", *rv_message)

        rv_resp_message = (1, True)
        rvr_packed = message.build_request_vote_response(*rv_resp_message)
        self.test_server._handle_as_candidate("test_identifier", rvr_packed)
        self.test_server._process_request_vote_response.\
            assert_called_once_with("test_identifier", *rv_resp_message)

    def test_process_append_entry_request(self):

        self.test_server._remote_servers = mock.MagicMock()
        test_peer = mock.Mock(spec=peer.Peer)
        self.test_server._remote_servers.__getitem__.return_value = test_peer

        # stale term
        ae_req_message = (-1, 2, 3, 4, ())
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req_message)
        test_ae_response = message.build_append_entry_response(
            self.test_server._current_term, False)
        self.test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(test_ae_response)
        self.test_server._remote_servers.__getitem__.reset_mock()
        test_peer.reset_mock()

        # current term outdated
        ae_req_message = (2, 2, 3, 4, ())
        self.test_server._switch_to_follower = mock.Mock()
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req_message)
        self.test_server._switch_to_follower.assert_called_once_with(
            2, "test_identifier")
        self.test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_ae_response = message.build_append_entry_response(
            self.test_server._current_term, False)
        test_peer.send_message.assert_called_once_with(test_ae_response)
        self.test_server._remote_servers.__getitem__.reset_mock()
        test_peer.reset_mock()
        self.test_server._switch_to_follower.reset_mock()

        # equals terms as follower
        ae_req_message = (0, -1, -1, 4, ())
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req_message)
        assert self.test_server._leader == "test_identifier"
        test_ae_response = message.build_append_entry_response(
            self.test_server._current_term, True)
        test_peer.send_message.assert_called_once_with(test_ae_response)
        test_peer.reset_mock()

        # equals terms as leader
        ae_req_message = (0, -1, -1, 4, ())
        self.test_server._state = raft.Raft.LEADER
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req_message)
        assert self.test_server._leader == "test_identifier"
        self.test_server._switch_to_follower.assert_called_once_with(
            0, None)
        self.test_server._switch_to_follower.reset_mock()

        # equals terms as candidate
        ae_req_message = (0, -1, -1, 4, ())
        self.test_server._state = raft.Raft.CANDIDATE
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req_message)
        assert self.test_server._leader == "test_identifier"
        self.test_server._switch_to_follower.assert_called_once_with(
            0, "test_identifier")
        test_ae_response = message.build_append_entry_response(
            self.test_server._current_term, True)
        test_peer.send_message.assert_called_once_with(test_ae_response)

    # TODO(yassine)
    def test_process_append_entry_response(self):
        pass

    def test_process_request_vote(self):

        self.test_server._remote_servers = mock.MagicMock()
        test_peer = mock.Mock(spec=peer.Peer)
        self.test_server._remote_servers.__getitem__.return_value = test_peer

        # stale term
        ae_req_message = (-1, 2, 3)
        self.test_server._process_request_vote("test_identifier",
                                               *ae_req_message)
        test_rv_response = message.build_request_vote_response(
            self.test_server._current_term, False)
        self.test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(test_rv_response)
        self.test_server._remote_servers.__getitem__.reset_mock()
        test_peer.reset_mock()

        # current term outdated
        ae_req_message = (2, 2, 3)
        self.test_server._switch_to_follower = mock.Mock()
        self.test_server._process_request_vote("test_identifier",
                                               *ae_req_message)
        test_rv_response = message.build_request_vote_response(
            self.test_server._current_term, True)
        self.test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(test_rv_response)
        self.test_server._remote_servers.__getitem__.reset_mock()
        test_peer.reset_mock()
        self.test_server._switch_to_follower.reset_mock()

        # equals term, not voted
        ae_req_message = (0, -1, -1)
        self.test_server._voted_for = None
        self.test_server._process_request_vote("test_identifier",
                                               *ae_req_message)
        test_rv_response = message.build_request_vote_response(
            self.test_server._current_term, True)
        self.test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(test_rv_response)
        self.test_server._remote_servers.__getitem__.reset_mock()
        test_peer.reset_mock()
        assert self.test_server._voted_for == "test_identifier"

        # equals term, voted
        ae_req_message = (0, -1, -1)
        self.test_server._process_request_vote("test_identifier",
                                               *ae_req_message)
        test_rv_response = message.build_request_vote_response(
            self.test_server._current_term, False)
        self.test_server._remote_servers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(test_rv_response)

    def test_process_request_vote_response(self):
        self.test_server = raft.Raft("127.0.0.1:2406")

        # stale term
        self.test_server._switch_to_follower = mock.Mock()
        self.test_server._switch_to_leader = mock.Mock()
        self.test_server._switch_to_candidate = mock.Mock()
        self.test_server._process_request_vote_response("test_identifier",
                                                        -1, True)
        assert self.test_server._switch_to_follower.call_count == 0
        assert self.test_server._switch_to_follower.call_count == 0
        assert self.test_server._switch_to_candidate.call_count == 0

        # current term outdated
        self.test_server._process_request_vote_response("test_identifier",
                                                        1, True)
        self.test_server._switch_to_follower.assert_called_once_with(1, None)
        self.test_server._switch_to_follower.reset_mock()

        # equals term with request vote granted with quorum
        assert len(self.test_server._voters) == 0
        self.test_server._process_request_vote_response("test_identifier",
                                                        0, True)
        assert self.test_server._voters == {"test_identifier"}
        self.test_server._switch_to_leader.assert_called_once_with()
        self.test_server._switch_to_leader.reset_mock()

        # equals term with request vote denied
        self.test_server._process_request_vote_response("test_identifier",
                                                        0, False)
        assert self.test_server._switch_to_follower.call_count == 0
        assert self.test_server._switch_to_follower.call_count == 0
        assert self.test_server._switch_to_candidate.call_count == 0

    def test_send_heartbeat(self):
        pytest.raises(raft.InvalidState, self.test_server._send_heartbeat)

        self.test_server._state = raft.Raft.LEADER
        self.test_server._broadcast_message = mock.Mock()
        self.test_server._send_heartbeat()
        test_heartbeat = message.build_append_entry_request(
            self.test_server._current_term, -1, -1, 0, ())
        self.test_server._broadcast_message.assert_called_once_with(
            test_heartbeat)

    def test_election_timeout_task(self):
        self.test_server = raft.Raft("127.0.0.1:2406")

        self.test_server._state = raft.Raft.LEADER
        pytest.raises(raft.InvalidState,
                      self.test_server._election_timeout_task)

        # with one node
        self.test_server._state = raft.Raft.FOLLOWER
        self.test_server._switch_to_leader = mock.Mock()
        self.test_server._switch_to_candidate = mock.Mock()
        self.test_server._election_timeout_task()
        self.test_server._switch_to_leader.assert_called_once_with()
        assert self.test_server._switch_to_candidate.call_count == 0

        # with several nodes
        self.test_server = raft.Raft("127.0.0.1:2406", {"127.0.0.1:2407",
                                                        "127.0.0.1:2408"})
        self.test_server._state = raft.Raft.FOLLOWER
        self.test_server._switch_to_leader = mock.Mock()
        self.test_server._switch_to_candidate = mock.Mock()
        self.test_server._election_timeout_task()
        self.test_server._switch_to_candidate.assert_called_once_with()
        assert self.test_server._switch_to_leader.call_count == 0
        assert self.test_server._leader is None

    def test_broadcast_message(self):

        test_peer_1 = mock.Mock(spec=peer.Peer)
        test_peer_2 = mock.Mock(spec=peer.Peer)
        self.test_server._remote_servers["127.0.0.1:2407"] = test_peer_1
        self.test_server._remote_servers["127.0.0.1:2408"] = test_peer_2

        self.test_server._broadcast_message(b"test_message")
        test_peer_1.send_message.assert_called_once_with(b"test_message")
        test_peer_2.send_message.assert_called_once_with(b"test_message")

    def test_switch_to_leader(self):

        pytest.raises(raft.InvalidState, self.test_server._switch_to_leader)

        self.test_server._state = raft.Raft.CANDIDATE

        self.test_server._send_heartbeat = mock.Mock()
        self.test_server._heartbeating = mock.Mock()
        self.test_server._check_leader_timeout = mock.Mock()

        self.test_server._switch_to_leader()

        assert self.test_server._state == raft.Raft.LEADER
        assert len(self.test_server._voters) == 0
        assert self.test_server._voted_for is None

        self.test_server._send_heartbeat.assert_called_once_with()
        self.test_server._send_heartbeat.reset_mock()
        self.test_server._heartbeating.start.assert_called_once_with()
        self.test_server._heartbeating.reset_mock()
        self.test_server._check_leader_timeout.stop.assert_called_once_with()
        self.test_server._check_leader_timeout.reset_mock()

        self.test_server._remote_server_endpoints = {}
        self.test_server._state = raft.Raft.CANDIDATE
        self.test_server._switch_to_leader()
        assert self.test_server._send_heartbeat.call_count == 0
        assert self.test_server._heartbeating.start.call_count == 0
        self.test_server._check_leader_timeout.stop.assert_called_once_with()

    def test_switch_to_follower(self):

        self.test_server._state = raft.Raft.LEADER

        self.test_server._heartbeating = mock.Mock()
        self.test_server._check_leader_timeout = mock.Mock()

        self.test_server._switch_to_follower(1, "new_leader")

        assert self.test_server._state == raft.Raft.FOLLOWER
        assert self.test_server._leader == "new_leader"
        assert self.test_server._current_term == 1
        assert len(self.test_server._voters) == 0
        assert self.test_server._voted_for is None

        self.test_server._heartbeating.stop.assert_called_once_with()
        self.test_server._check_leader_timeout.start.assert_called_once_with()

    def test_switch_to_candidate(self):

        self.test_server._state = -1
        pytest.raises(raft.InvalidState, self.test_server._switch_to_candidate)

        self.test_server._state = raft.Raft.FOLLOWER
        self.test_server._broadcast_message = mock.Mock()
        self.test_server._check_leader_timeout = mock.Mock()

        self.test_server._switch_to_candidate()

        assert (self.test_server._voters ==
                {self.test_server._local_server_endpoint})
        assert self.test_server._state == raft.Raft.CANDIDATE
        assert (self.test_server._voted_for ==
                self.test_server._local_server_endpoint)
        rv_message = message.build_request_vote(self.test_server._current_term,
                                                -1, -1)
        self.test_server._broadcast_message.assert_called_once_with(rv_message)
        assert self.test_server._check_leader_timeout.callback_time != 0
