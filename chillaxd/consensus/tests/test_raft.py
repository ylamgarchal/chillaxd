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
import signal
import six
import zmq
from chillaxd.consensus import peer
from chillaxd.consensus import raft

from chillaxd.consensus import message


class TestServer(object):

    _DEFAULT_ARGUMENTS = {'public_endpoint': '127.0.0.1:27001',
                          'private_endpoint': '127.0.0.1:2406',
                          'remote_endpoints': ['127.0.0.1:2407',
                                               '127.0.0.1:2408'],
                          'leader_heartbeat_interval': 50,
                          'min_election_timeout': 200,
                          'max_election_timeout': 300}

    def setup_method(self, method):

        self.test_server = raft.Raft(**TestServer._DEFAULT_ARGUMENTS)

    @mock.patch("chillaxd.consensus.raft.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.consensus.raft.zmq.Context", spec=zmq.Context)
    def test_setup(self, m_zmq_context, m_zmq_ioloop):

        self.test_server._setup()
        m_zmq_context.assert_called_once_with()

        remote_server_endpoints = self.test_server._remote_endpoints
        for remote_server_endpoint in remote_server_endpoints:
            binary_r_e = six.b(remote_server_endpoint)

            assert isinstance(self.test_server._remote_peers[binary_r_e],
                              peer.Peer)
            assert self.test_server._next_index[binary_r_e] == 1
            assert self.test_server._match_index[binary_r_e] == 0

        assert len(self.test_server._remote_peers) == 2

        ioloop_instance = m_zmq_ioloop().instance
        ioloop_instance.assert_called_once_with()
        assert ioloop_instance().add_handler.call_count == 2

        assert isinstance(self.test_server._checking_leader_timeout,
                          zmq.eventloop.ioloop.PeriodicCallback)

        assert (self.test_server._checking_leader_timeout.callback ==
                self.test_server._election_timeout_task)

        assert (self.test_server._heartbeating.callback ==
                self.test_server._broadcast_ae_heartbeat)

    @mock.patch("chillaxd.consensus.raft.signal", spec=signal)
    @mock.patch(
        "chillaxd.consensus.raft.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.consensus.raft.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.consensus.raft.zmq.Context", spec=zmq.Context)
    def test_start(self, m_zmq_context, m_zmq_ioloop, m_zmq_periodic_callback,
                   m_signal):
        self.test_server.start()
        assert m_zmq_context().socket().bind.call_count == 2
        assert m_signal.signal.call_count == 2
        for remote_server in six.itervalues(self.test_server._remote_peers):
                assert remote_server._is_started is True
        assert self.test_server._is_started is True
        m_zmq_ioloop().instance().start.assert_called_once_with()

    @mock.patch("chillaxd.consensus.raft.signal", spec=signal)
    @mock.patch(
        "chillaxd.consensus.raft.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.consensus.raft.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.consensus.raft.zmq.Context", spec=zmq.Context)
    def test_stop(self, m_zmq_context, m_zmq_ioloop, m_zmq_periodic_callback,
                  m_signal):
        self.test_server.start()

        self.test_server._checking_leader_timeout = mock.Mock()
        self.test_server._heartbeating = mock.Mock()
        self.test_server._socket_for_commands = mock.Mock()
        self.test_server._socket_for_consensus = mock.Mock()

        self.test_server.stop()

        (self.test_server._checking_leader_timeout.stop.
         assert_called_once_with())
        self.test_server._heartbeating.stop.assert_called_once_with()
        self.test_server._socket_for_commands.close.assert_called_once_with()
        self.test_server._socket_for_consensus.close.assert_called_once_with()

        for remote_server in six.itervalues(self.test_server._remote_peers):
                assert remote_server._is_started is False

        m_zmq_context().destroy.assert_called_once_with(linger=0)
        m_zmq_ioloop().instance().stop.assert_called_once_with()
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

    def test_is_standalone(self):

        assert self.test_server._is_standalone() is False
        self.test_server._remote_endpoints = {}
        assert self.test_server._is_standalone() is True

    def test_dispatch_internal_raft_message(self):

        self.test_server._process_append_entry_request = mock.Mock()
        self.test_server._process_append_entry_response = mock.Mock()
        self.test_server._process_request_vote = mock.Mock()
        self.test_server._process_request_vote_response = mock.Mock()
        mock_socket = mock.Mock(spec=zmq.sugar.socket.Socket)

        # Append entry request.
        aereq = (1, 2, 3, 4, ())
        aereq_packed = message.build_append_entry_request(*aereq)
        mock_socket.recv_multipart.return_value = ("identifier", aereq_packed)
        self.test_server._dispatch_internal_raft_message(mock_socket,
                                                         zmq.POLLIN)
        self.test_server._process_append_entry_request.assert_called_once_with(
            "identifier", *aereq)

        # Append entry response.
        aeresp = (1, True, 0)
        aeresp_packed = message.build_append_entry_response(*aeresp)
        mock_socket.recv_multipart.return_value = ("identifier", aeresp_packed)
        self.test_server._dispatch_internal_raft_message(mock_socket,
                                                         zmq.POLLIN)
        self.test_server._process_append_entry_response.\
            assert_called_once_with("identifier", *aeresp)

        # Request vote.
        rv = (1, 2, 3)
        rv_packed = message.build_request_vote(*rv)
        mock_socket.recv_multipart.return_value = ("identifier", rv_packed)
        self.test_server._dispatch_internal_raft_message(mock_socket,
                                                         zmq.POLLIN)
        self.test_server._process_request_vote.assert_called_once_with(
            "identifier", *rv)

        # Request vote response.
        rvresp = (0, False)
        rvresp_packed = message.build_request_vote_response(*rvresp)
        mock_socket.recv_multipart.return_value = ("identifier", rvresp_packed)
        self.test_server._dispatch_internal_raft_message(mock_socket,
                                                         zmq.POLLIN)
        self.test_server._process_request_vote_response.\
            assert_called_once_with("identifier", *rvresp)

    def test_process_append_entry_request(self):

        self.test_server._remote_peers = mock.MagicMock()
        test_peer = mock.Mock(spec=peer.Peer)
        self.test_server._remote_peers.__getitem__.return_value = test_peer

        # stale term
        ae_req = (-1, 2, 3, 4, ())
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req)
        ae_response = message.build_append_entry_response(
            self.test_server._current_term, False, None)
        self.test_server._remote_peers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(ae_response)
        self.test_server._remote_peers.__getitem__.reset_mock()
        test_peer.reset_mock()

        # current term outdated
        ae_req = (2, 2, 3, 4, ())
        self.test_server._switch_to_follower = mock.Mock()
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req)
        self.test_server._switch_to_follower.assert_called_once_with(
            2, "test_identifier")
        self.test_server._remote_peers.__getitem__.assert_called_once_with(
            "test_identifier")
        ae_response = message.build_append_entry_response(
            self.test_server._current_term, False, None)
        test_peer.send_message.assert_called_once_with(ae_response)
        self.test_server._remote_peers.__getitem__.reset_mock()
        test_peer.reset_mock()
        self.test_server._switch_to_follower.reset_mock()

        # equals terms as follower
        ae_req = (0, 0, 0, 4, ())
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req)
        assert self.test_server._leader == "test_identifier"
        ae_response = message.build_append_entry_response(
            self.test_server._current_term, True, 0)
        test_peer.send_message.assert_called_once_with(ae_response)
        test_peer.reset_mock()

        # equals terms as leader
        ae_req = (0, 0, 0, 4, ())
        self.test_server._state = raft.Raft._LEADER
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req)
        assert self.test_server._leader == "test_identifier"
        self.test_server._switch_to_follower.assert_called_once_with(
            0, None)
        self.test_server._switch_to_follower.reset_mock()

        # equals terms as candidate
        ae_req = (0, 0, 0, 4, ())
        self.test_server._state = raft.Raft._CANDIDATE
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req)
        assert self.test_server._leader == "test_identifier"
        self.test_server._switch_to_follower.assert_called_once_with(
            0, "test_identifier")
        ae_response = message.build_append_entry_response(
            self.test_server._current_term, True, 0)
        test_peer.send_message.assert_called_once_with(ae_response)
        test_peer.reset_mock()

        # equals terms with induction failed
        ae_req = (0, -1, -1, 4, ())
        self.test_server._state = raft.Raft._FOLLOWER
        self.test_server._process_append_entry_request("test_identifier",
                                                       *ae_req)
        assert self.test_server._leader == "test_identifier"
        ae_response = message.build_append_entry_response(
            self.test_server._current_term, False, None)
        test_peer.send_message.assert_called_once_with(ae_response)

    # TODO(yassine)
    def test_process_append_entry_response(self):
        pass

    def test_process_request_vote(self):

        self.test_server._remote_peers = mock.MagicMock()
        test_peer = mock.Mock(spec=peer.Peer)
        self.test_server._remote_peers.__getitem__.return_value = test_peer

        # stale term
        ae_req_message = (-1, 2, 3)
        self.test_server._process_request_vote("test_identifier",
                                               *ae_req_message)
        rv_response = message.build_request_vote_response(
            self.test_server._current_term, False)
        self.test_server._remote_peers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(rv_response)
        self.test_server._remote_peers.__getitem__.reset_mock()
        test_peer.reset_mock()

        # current term outdated
        ae_req_message = (2, 2, 3)
        self.test_server._switch_to_follower = mock.Mock()
        self.test_server._process_request_vote("test_identifier",
                                               *ae_req_message)
        rv_response = message.build_request_vote_response(
            self.test_server._current_term, True)
        self.test_server._remote_peers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(rv_response)
        self.test_server._remote_peers.__getitem__.reset_mock()
        test_peer.reset_mock()
        self.test_server._switch_to_follower.reset_mock()

        # equals term, not voted
        ae_req_message = (0, 1, 1)
        self.test_server._voted_for = None
        self.test_server._process_request_vote("test_identifier",
                                               *ae_req_message)
        rv_response = message.build_request_vote_response(
            self.test_server._current_term, True)
        self.test_server._remote_peers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(rv_response)
        self.test_server._remote_peers.__getitem__.reset_mock()
        test_peer.reset_mock()
        assert self.test_server._voted_for == "test_identifier"

        # equals term, voted
        ae_req_message = (0, -1, -1)
        self.test_server._process_request_vote("test_identifier",
                                               *ae_req_message)
        rv_response = message.build_request_vote_response(
            self.test_server._current_term, False)
        self.test_server._remote_peers.__getitem__.assert_called_once_with(
            "test_identifier")
        test_peer.send_message.assert_called_once_with(rv_response)

    def test_process_request_vote_response(self):

        test_args = TestServer._DEFAULT_ARGUMENTS.copy()
        test_args["remote_endpoints"] = ["127.0.0.1:2406"]
        self.test_server = raft.Raft(**test_args)

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
        self.test_server._state = raft.Raft._CANDIDATE
        self.test_server._process_request_vote_response("test_identifier",
                                                        1, True)
        self.test_server._switch_to_follower.assert_called_once_with(1, None)
        self.test_server._switch_to_follower.reset_mock()

        # equals term with request vote granted with quorum
        self.test_server._voters = {"self.test_server"}
        assert len(self.test_server._voters) == 1
        self.test_server._process_request_vote_response("test_identifier",
                                                        0, True)
        assert len(self.test_server._voters) == 2
        self.test_server._switch_to_leader.assert_called_once_with()
        self.test_server._switch_to_leader.reset_mock()

        # equals term with request vote denied
        self.test_server._process_request_vote_response("test_identifier",
                                                        0, False)
        assert self.test_server._switch_to_follower.call_count == 0
        assert self.test_server._switch_to_follower.call_count == 0
        assert self.test_server._switch_to_candidate.call_count == 0

    def test_broadcast_ae_heartbeat(self):
        assert self.test_server._state != raft.Raft._LEADER
        pytest.raises(raft.InvalidState,
                      self.test_server._broadcast_ae_heartbeat)

        self.test_server._state = raft.Raft._LEADER
        ae_heartbeat = message.build_append_entry_request(
            self.test_server._current_term, 0, 0, 0, ())

        for remote_server in range(3):
            ts = self.test_server
            ts._remote_peers[remote_server] = mock.Mock()
            ts._next_index[remote_server] = 1
            ts._log = mock.Mock()
            ts._log.prev_index_and_term_of_entry.return_value = (0, 0)
            ts._log.entries_from_index.return_value = ()

        self.test_server._broadcast_ae_heartbeat()

        for remote_server in range(3):
            m_remote_server = self.test_server._remote_peers[remote_server]
            m_remote_server.send_message.assert_called_once_with(ae_heartbeat)

    def test_election_timeout_task(self):
        test_args = TestServer._DEFAULT_ARGUMENTS.copy()
        test_args["remote_endpoints"] = []
        self.test_server = raft.Raft(**test_args)

        self.test_server._state = raft.Raft._LEADER
        pytest.raises(raft.InvalidState,
                      self.test_server._election_timeout_task)

        # with one node
        self.test_server._state = raft.Raft._FOLLOWER
        self.test_server._switch_to_leader = mock.Mock()
        self.test_server._switch_to_candidate = mock.Mock()
        self.test_server._election_timeout_task()
        self.test_server._switch_to_leader.assert_called_once_with()
        assert self.test_server._switch_to_candidate.call_count == 0

        # with several nodes
        test_args["remote_endpoints"] = ["127.0.0.1:2407", "127.0.0.1:2408"]
        self.test_server = raft.Raft(**test_args)
        self.test_server._state = raft.Raft._FOLLOWER
        self.test_server._switch_to_leader = mock.Mock()
        self.test_server._switch_to_candidate = mock.Mock()
        self.test_server._election_timeout_task()
        self.test_server._switch_to_candidate.assert_called_once_with()
        assert self.test_server._switch_to_leader.call_count == 0
        assert self.test_server._leader is None

    def test_switch_to_leader(self):

        pytest.raises(raft.InvalidState, self.test_server._switch_to_leader)

        self.test_server._broadcast_ae_heartbeat = mock.Mock()
        self.test_server._heartbeating = mock.Mock()
        self.test_server._checking_leader_timeout = mock.Mock()

        self.test_server._state = raft.Raft._CANDIDATE
        self.test_server._switch_to_leader()

        assert self.test_server._state == raft.Raft._LEADER
        assert len(self.test_server._voters) == 0
        assert self.test_server._voted_for is None

        self.test_server._broadcast_ae_heartbeat.assert_called_once_with()
        self.test_server._heartbeating.start.assert_called_once_with()
        self.test_server._heartbeating.reset_mock()
        checking_leader_timeout = self.test_server._checking_leader_timeout
        checking_leader_timeout.stop.assert_called_once_with()
        self.test_server._checking_leader_timeout.reset_mock()
        self.test_server._broadcast_ae_heartbeat.reset_mock()

        self.test_server._remote_endpoints = {}
        self.test_server._state = raft.Raft._CANDIDATE
        self.test_server._switch_to_leader()
        assert self.test_server._broadcast_ae_heartbeat.call_count == 0
        assert self.test_server._heartbeating.start.call_count == 0
        checking_leader_timeout = self.test_server._checking_leader_timeout
        checking_leader_timeout.stop.assert_called_once_with()

    def test_switch_to_follower(self):

        self.test_server._state = raft.Raft._LEADER
        assert self.test_server._state != raft.Raft._FOLLOWER

        self.test_server._heartbeating = mock.Mock()
        self.test_server._checking_leader_timeout = mock.Mock()

        self.test_server._switch_to_follower(1, "new_leader")

        assert self.test_server._state == raft.Raft._FOLLOWER
        assert self.test_server._leader == "new_leader"
        assert self.test_server._current_term == 1
        assert len(self.test_server._voters) == 0
        assert self.test_server._voted_for is None

        self.test_server._heartbeating.stop.assert_called_once_with()
        checking_leader_timeout = self.test_server._checking_leader_timeout
        checking_leader_timeout.start.assert_called_once_with()

    def test_switch_to_candidate(self):

        self.test_server._state = raft.Raft._LEADER

        pytest.raises(raft.InvalidState, self.test_server._switch_to_candidate)

        self.test_server._state = raft.Raft._FOLLOWER
        self.test_server._broadcast_ae_heartbeat = mock.Mock()
        self.test_server._checking_leader_timeout = mock.Mock()
        for remote_server in range(3):
            self.test_server._remote_peers[remote_server] = mock.Mock()

        self.test_server._switch_to_candidate()

        assert (self.test_server._voters ==
                {self.test_server._private_endpoint})
        assert self.test_server._state == raft.Raft._CANDIDATE
        assert (self.test_server._voted_for ==
                self.test_server._private_endpoint)
        rv = message.build_request_vote(self.test_server._current_term, 0, 0)

        for remote_server in range(3):
            m_remote_server = self.test_server._remote_peers[remote_server]
            m_remote_server.send_message.assert_called_once_with(rv)
