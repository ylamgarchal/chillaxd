# -*- coding: utf-8 -*-
# Copyright Yassine Lamgarchal <lamgarchal.yassine@gmail.com>
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

from chillaxd.raft import message
from chillaxd.raft import peer
from chillaxd.raft import server
from chillaxd.raft import serverstate


class TestServer(object):

    _DEFAULT_ARGUMENTS = {'public_endpoint': '127.0.0.1:27001',
                          'private_endpoint': '127.0.0.1:2406',
                          'remote_endpoints': ['127.0.0.1:2407',
                                               '127.0.0.1:2408'],
                          'leader_heartbeat_interval': 50,
                          'min_election_timeout': 200,
                          'max_election_timeout': 300}

    def setup_method(self, method):

        self.server = server.RaftServer(**TestServer._DEFAULT_ARGUMENTS)
        self.server._remote_peers = mock.MagicMock()
        self.server._server_state = mock.Mock()
        self.server._server_state.term.return_value = 0
        self.server._apply_committed_log_entries_to_state_machine = \
            mock.Mock()
        self.server._remote_peers.__getitem__.return_value = \
            mock.Mock(spec=peer.Peer)

    @mock.patch("chillaxd.raft.server.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.raft.server.zmq.Context", spec=zmq.Context)
    def test_setup(self, m_zmq_context, m_zmq_ioloop):

        self.server._remote_peers = {}
        self.server._setup()
        m_zmq_context.assert_called_once_with()

        assert self.server._server_state.init_indexes.call_count == 2
        assert len(self.server._remote_peers) == 2

        ioloop_instance = m_zmq_ioloop().instance
        ioloop_instance.assert_called_once_with()
        assert ioloop_instance().add_handler.call_count == 2

        assert isinstance(self.server.checking_leader_timeout,
                          zmq.eventloop.ioloop.PeriodicCallback)

        assert (self.server.checking_leader_timeout.callback ==
                self.server._election_timeout_task)

        assert (self.server.heartbeating.callback ==
                self.server.broadcast_append_entries)

    @mock.patch("chillaxd.raft.server.signal", spec=signal)
    @mock.patch(
        "chillaxd.raft.server.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.raft.server.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.raft.server.zmq.Context", spec=zmq.Context)
    def test_start(self, m_zmq_context, m_zmq_ioloop, m_zmq_periodic_callback,
                   m_signal):
        self.server.start()
        assert m_zmq_context().socket().bind.call_count == 2
        assert m_signal.signal.call_count == 2
        for remote_server in six.itervalues(self.server._remote_peers):
                assert remote_server._is_started is True
        assert self.server._is_started is True
        m_zmq_ioloop().instance().start.assert_called_once_with()

    @mock.patch("chillaxd.raft.server.signal", spec=signal)
    @mock.patch(
        "chillaxd.raft.server.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.raft.server.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.raft.server.zmq.Context", spec=zmq.Context)
    def test_stop(self, m_zmq_context, m_zmq_ioloop, m_zmq_periodic_callback,
                  m_signal):
        self.server.start()

        self.server.checking_leader_timeout = mock.Mock()
        self.server.heartbeating = mock.Mock()
        self.server._socket_for_commands = mock.Mock()
        self.server._socket_for_consensus = mock.Mock()

        self.server.stop()

        (self.server.checking_leader_timeout.stop.
         assert_called_once_with())
        self.server.heartbeating.stop.assert_called_once_with()
        self.server._socket_for_commands.close.assert_called_once_with()
        self.server._socket_for_consensus.close.assert_called_once_with()

        for remote_server in six.itervalues(self.server._remote_peers):
                assert remote_server._is_started is False

        m_zmq_context().destroy.assert_called_once_with(linger=0)
        m_zmq_ioloop().instance().stop.assert_called_once_with()
        assert self.server._is_started is False

    @mock.patch(
        "chillaxd.raft.server.zmq.eventloop.ioloop.PeriodicCallback",
        spec=zmq.eventloop.ioloop.PeriodicCallback)
    @mock.patch("chillaxd.raft.server.zmq.eventloop.ioloop.ZMQIOLoop",
                spec=zmq.eventloop.ioloop.ZMQIOLoop)
    @mock.patch("chillaxd.raft.server.zmq.Context", spec=zmq.Context)
    def test_handle_signals(self, zmq_context, zmq_ioloop,
                            zmq_periodic_callback):
        self.server.start()
        assert self.server._is_started is True
        self.server._handle_signals(None, None)
        assert self.server._is_started is False

    def test_is_standalone(self):

        assert self.server.is_standalone() is False
        self.server._remote_endpoints = {}
        assert self.server.is_standalone() is True

    def test_process_internal_raft_message(self):

        self.server._process_append_entry_request = mock.Mock()
        self.server._process_append_entry_response = mock.Mock()
        self.server._process_request_vote = mock.Mock()
        self.server._process_request_vote_response = mock.Mock()
        mock_socket = mock.Mock(spec=zmq.sugar.socket.Socket)

        # Append entry request.
        aereq = (1, 2, 3, 4, ())
        aereq_packed = message.build_append_entry_request(*aereq)
        mock_socket.recv_multipart.return_value = ("identifier", aereq_packed)
        self.server._process_internal_message(mock_socket, zmq.POLLIN)
        self.server._process_append_entry_request.assert_called_once_with(
            "identifier", *aereq)

        # Append entry response.
        aeresp = (1, True, 0, None)
        aeresp_packed = message.build_append_entry_response(*aeresp)
        mock_socket.recv_multipart.return_value = ("identifier", aeresp_packed)
        self.server._process_internal_message(mock_socket, zmq.POLLIN)
        self.server._process_append_entry_response.\
            assert_called_once_with("identifier", *aeresp)

        # Request vote.
        rv = (1, 2, 3)
        rv_packed = message.build_request_vote(*rv)
        mock_socket.recv_multipart.return_value = ("identifier", rv_packed)
        self.server._process_internal_message(mock_socket, zmq.POLLIN)
        self.server._process_request_vote.assert_called_once_with(
            "identifier", *rv)

        # Request vote response.
        rvresp = (0, False)
        rvresp_packed = message.build_request_vote_response(*rvresp)
        mock_socket.recv_multipart.return_value = ("identifier", rvresp_packed)
        self.server._process_internal_message(mock_socket, zmq.POLLIN)
        self.server._process_request_vote_response.\
            assert_called_once_with("identifier", *rvresp)

    def test_process_append_entry_request_stale_term(self):

        ae_req = (-1, 2, 3, 4, ())
        self.server._process_append_entry_request("peer_id", *ae_req)

        ae_response = message.build_append_entry_response(
            self.server._server_state.term(), False, None, None)
        self.server._remote_peers["peer_id"].send_message.\
            assert_called_once_with(ae_response)
        self.server._remote_peers["peer_id"].send_message.reset_mock()

    def test_process_append_entry_request_outdated_term(self):

        ae_req = (1, 2, 3, 4, ())
        self.server._process_append_entry_request("peer_id", *ae_req)

        self.server._server_state.switch_to_follower.\
            assert_called_once_with(1, "peer_id")
        self.server._server_state.switch_to_follower.reset_mock()
        ae_response = message.build_append_entry_response(
            self.server._server_state.term(), False, None, None)
        self.server._remote_peers["peer_id"].send_message.\
            assert_called_once_with(ae_response)
        self.server._remote_peers["peer_id"].send_message.reset_mock()

    def test_process_append_entry_request_as_leader(self):
        ae_req = (0, 0, 0, 4, ())
        self.server._server_state.is_leader.return_value = True
        self.server._process_append_entry_request("peer_id", *ae_req)
        self.server._server_state.switch_to_follower.\
            assert_called_once_with(0, None)
        self.server._server_state.switch_to_follower.reset_mock()

    def test_process_append_entry_request_as_follower(self):
        ae_req = (0, 0, 0, 4, ())
        self.server._server_state.is_leader.return_value = False
        self.server._server_state.commit_index.return_value = 4
        self.server._process_append_entry_request("peer_id", *ae_req)
        self.server._server_state.update_leader.assert_called_once_with(
            "peer_id")
        self.server._server_state.update_leader.reset_mock()
        ae_response = message.build_append_entry_response(
            self.server._server_state.term(), True, 0, None)
        self.server._remote_peers["peer_id"].\
            send_message.assert_called_once_with(ae_response)
        self.server._remote_peers["peer_id"].\
            send_message.reset_mock()

    def test_process_append_entry_request_as_candidate(self):
        ae_req = (0, 0, 0, 4, ())
        self.server._server_state.is_leader.return_value = False
        self.server._server_state.is_candidate.return_value = True
        self.server._server_state.commit_index.return_value = 4
        self.server._process_append_entry_request("peer_id", *ae_req)
        self.server._server_state.update_leader.assert_called_once_with(
            "peer_id")
        self.server._server_state.update_leader.reset_mock()
        self.server._server_state.switch_to_follower.\
            assert_called_once_with(0, "peer_id")
        self.server._server_state.switch_to_follower.reset_mock()
        ae_response = message.build_append_entry_response(
            self.server._server_state.term(), True, 0, None)
        self.server._remote_peers["peer_id"].\
            send_message.assert_called_once_with(ae_response)
        self.server._remote_peers["peer_id"].send_message.reset_mock()

    def test_process_append_entry_request_induction_failed(self):
        ae_req = (0, -1, -1, 4, ())
        self.server._server_state.is_leader.return_value = False
        self.server._process_append_entry_request("peer_id", *ae_req)
        ae_response = message.build_append_entry_response(
            self.server._server_state.term(), False, None, 0)
        self.server._remote_peers["peer_id"].send_message.\
            assert_called_once_with(ae_response)
        self.server._remote_peers["peer_id"].send_message.reset_mock()

    def test_process_append_entry_request_induction_succeed(self):
        pass

    # TODO(yassine)
    def test_process_append_entry_response(self):
        pass

    def test_process_request_vote_stale_term(self):

        ae_req_message = (-1, 2, 3)
        self.server._process_request_vote("peer_id", *ae_req_message)
        rv_response_ko = message.build_request_vote_response(
            self.server._server_state.term(), False)
        self.server._remote_peers["peer_id"].send_message.\
            assert_called_once_with(rv_response_ko)
        self.server._remote_peers.reset_mock()

    def test_process_request_vote_outdated_term_grant_vote(self):

        ae_req_message = (2, 2, 3)
        self.server._server_state.switch_to_follower = mock.Mock()
        self.server._is_candidate_log_up_to_date = mock.Mock()
        self.server._is_candidate_log_up_to_date.return_value = True
        self.server._server_state.grant_vote = mock.Mock()
        self.server._server_state.grant_vote.return_value = True
        self.server._process_request_vote("peer_id", *ae_req_message)

        rv_response = message.build_request_vote_response(
            self.server._server_state.term(), True)
        self.server._server_state.switch_to_follower.\
            assert_called_once_with(2, None)
        self.server._remote_peers["peer_id"].send_message.\
            assert_called_once_with(rv_response)
        self.server._remote_peers["peer_id"].reset_mock()
        self.server._server_state.switch_to_follower.reset_mock()

    def test_process_request_vote_deny_vote(self):

        ae_req_message = (0, 1, 1)
        self.server._is_candidate_log_up_to_date = mock.Mock()
        self.server._is_candidate_log_up_to_date.return_value = True
        self.server._server_state.grant_vote = mock.Mock()
        self.server._server_state.grant_vote.return_value = False
        self.server._process_request_vote("peer_id", *ae_req_message)

        rv_response_ko = message.build_request_vote_response(
            self.server._server_state.term(), False)
        self.server._remote_peers["peer_id"].send_message.\
            assert_called_once_with(rv_response_ko)
        self.server._remote_peers["peer_id"].reset_mock()

    def test_process_request_vote_response_stale_term(self):

        test_args = TestServer._DEFAULT_ARGUMENTS.copy()
        test_args["remote_endpoints"] = ["peer_id"]
        self.server = server.RaftServer(**test_args)
        self.server._remote_peers = mock.MagicMock()
        self.server._server_state.is_candidate = mock.Mock()
        self.server._server_state.is_candidate.return_value = True
        self.server._process_request_vote_response("peer_id", -1, True)

    def test_process_request_vote_response_outdated_term(self):
        self.server._server_state.is_candidate.return_value = \
            serverstate.ServerState._CANDIDATE
        self.server._process_request_vote_response("peer_id", 1, True)
        self.server._server_state.switch_to_follower.\
            assert_called_once_with(1, None)
        self.server._server_state.switch_to_follower.reset_mock()

    def test_process_request_vote_response_granted(self):
        self.server._server_state._voters = {"peer_id"}
        self.server._server_state.number_of_voters.return_value = 2
        self.server._process_request_vote_response("peer_id2", 0, True)
        self.server._server_state.add_voter.\
            assert_called_once_with("peer_id2")
        self.server._server_state.switch_to_leader.\
            assert_called_once_with()
        self.server._server_state.switch_to_leader.reset_mock()

    def test_process_request_vote_response(self):
        # equals term with request vote denied
        self.server._process_request_vote_response("peer_id", 0, False)
        assert self.server._server_state.switch_to_follower.call_count == 0
        assert self.server._server_state.switch_to_follower.call_count == 0
        assert self.server._server_state.switch_to_candidate.call_count == 0

    def test_broadcast_append_entries(self):
        self.server._server_state.is_leader.return_value = False
        pytest.raises(serverstate.InvalidState,
                      self.server.broadcast_append_entries)

        self.server._server_state.is_leader.return_value = True
        ae_heartbeat = message.build_append_entry_request(
            self.server._server_state.term(), 0, 0, 0, ())

        self.server._server_state._next_index = mock.MagicMock()
        self.server._server_state._next_index.__getitem__.return_value = 1
        self.server._remote_peers = {0: mock.Mock(), 1: mock.Mock(),
                                     2: mock.Mock()}
        self.server._server_state.commit_index.return_value = 0
        self.server._server_state.term.return_value = 0

        for remote_server in six.iterkeys(self.server._remote_peers):
            ts = self.server
            ts._server_state._next_index[remote_server] = 1
            ts._log = mock.Mock()
            ts._log.prev_index_and_term_of_entry.return_value = (0, 0)
            ts._log.entries_from_index.return_value = ()

        self.server.broadcast_append_entries()

        for remote_server in range(3):
            m_remote_server = self.server._remote_peers[remote_server]
            m_remote_server.send_message.assert_called_once_with(ae_heartbeat)

    def test_election_timeout_task(self):
        test_args = TestServer._DEFAULT_ARGUMENTS.copy()
        test_args["remote_endpoints"] = []
        self.server = server.RaftServer(**test_args)
        self.server.checking_leader_timeout = mock.Mock()

        self.server._server_state.is_leader = mock.Mock()
        self.server._server_state.is_leader.return_value = True
        pytest.raises(serverstate.InvalidState,
                      self.server._election_timeout_task)
        self.server._server_state.is_leader.return_value = False

        # with one node
        self.server._state = serverstate.ServerState._FOLLOWER
        self.server._server_state.switch_to_leader = mock.Mock()
        self.server._server_state.switch_to_candidate = mock.Mock()
        self.server._election_timeout_task()
        self.server._server_state.switch_to_leader.\
            assert_called_once_with()
        assert self.server._server_state.switch_to_candidate.call_count == 0

        # with several nodes
        test_args["remote_endpoints"] = ["127.0.0.1:2407", "127.0.0.1:2408"]
        self.server = server.RaftServer(**test_args)
        self.server._state = serverstate.ServerState._FOLLOWER
        self.server._server_state.switch_to_leader = mock.Mock()
        self.server._server_state.switch_to_candidate = mock.Mock()
        self.server._server_state.update_leader = mock.Mock()
        self.server.checking_leader_timeout = mock.Mock()
        self.server._election_timeout_task()
        self.server._server_state.switch_to_candidate.\
            assert_called_once_with()
        self.server._server_state.switch_to_candidate.reset_mock()
        assert self.server._server_state.switch_to_leader.call_count == 0
        self.server._server_state.switch_to_leader.reset_mock()
        self.server._server_state.update_leader.\
            assert_called_once_with(None)
