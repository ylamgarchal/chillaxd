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

from chillaxd.raft import message
from chillaxd.raft import serverstate


class TestServerState(object):
    def setup_method(self, method):
        self.server = mock.Mock()
        self.log = mock.Mock()
        self.queued_commands = {}
        self.private_endpoint = "private_endpoint"
        self.server_state = serverstate.ServerState(self.server, self.log,
                                                    self.queued_commands,
                                                    self.private_endpoint)

    # TODO(yassine): add other test case
    def test_switch_to_leader_not_standalone(self):

        self.server.is_standalone.return_value = False
        self.server_state._current_state = serverstate.ServerState._FOLLOWER

        pytest.raises(serverstate.InvalidState,
                      self.server_state.switch_to_leader)

        self.server.remote_peers.return_value = ["peer_id1", "peer_id2"]
        self.log.last_index.return_value = 0
        self.server_state._current_state = serverstate.ServerState._CANDIDATE
        self.server_state.switch_to_leader()

        assert self.server_state.is_leader() is True
        assert len(self.server_state._voters) == 0
        assert self.server_state._voted_for is None

        self.server.broadcast_append_entries.assert_called_once_with()
        self.server.broadcast_append_entries.reset_mock()
        self.server.heartbeating.start.assert_called_once_with()
        self.server.heartbeating.reset_mock()
        self.server.checking_leader_timeout.stop.assert_called_once_with()
        self.server.checking_leader_timeout.reset_mock()

    # TODO(yassine): add other test case
    def test_switch_to_follower_as_leader(self):

        self.server_state._current_state = serverstate.ServerState._LEADER

        self.server_state.switch_to_follower(1, "new_leader")

        self.server.checking_leader_timeout.start.assert_called_once_with()
        self.server.checking_leader_timeout.reset_mock()
        self.server.heartbeating.stop.assert_called_once_with()
        self.server.heartbeating.reset_mock()
        assert self.server_state._current_state == \
            serverstate.ServerState._FOLLOWER
        assert self.server_state._leader == "new_leader"
        assert self.server_state._current_term == 1
        assert len(self.server_state._voters) == 0
        assert self.server_state._voted_for is None

    def test_switch_to_candidate(self):

        self.server_state._current_state = serverstate.ServerState._LEADER

        pytest.raises(serverstate.InvalidState,
                      self.server_state.switch_to_candidate)

        self.server_state._current_state = serverstate.ServerState._FOLLOWER
        self.log.index_and_term_of_last_entry.return_value = (0, 0)
        current_term = self.server_state._current_term
        self.server_state.switch_to_candidate()

        assert self.server_state._current_term == current_term + 1
        assert (self.server_state._voters ==
                {self.server_state._private_endpoint})
        assert self.server_state._current_state == \
            serverstate.ServerState._CANDIDATE
        assert (self.server_state._voted_for ==
                self.server_state._private_endpoint)
        rv = message.build_request_vote(self.server_state._current_term, 0, 0)

        self.server.broadcast_message.assert_called_once_with(rv)
        self.server.reset_election_timeout.assert_called_once_with()
