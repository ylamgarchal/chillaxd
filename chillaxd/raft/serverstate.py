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

import logging

from chillaxd import commands
from chillaxd.raft import message

LOG = logging.getLogger(__name__)


class ServerState(object):

    # The different Raft states.
    _LEADER = "LEADER"
    _CANDIDATE = "CANDIDATE"
    _FOLLOWER = "FOLLOWER"

    def __init__(self, server, log, queued_commands, private_endpoint):

        self._server = server
        self._log = log
        self._queued_commands = queued_commands
        self._private_endpoint = private_endpoint

        # The server state, initially a follower.
        self._current_state = ServerState._FOLLOWER

        # The current known leader.
        self._leader = None

        # Set of peers that voted for this server in current term.
        self._voters = set()

        # TODO(yassine): must be persisted
        # The candidate the server has voted for in current term.
        self._voted_for = None

        # Index of highest log entry known to be committed
        # (initialized to 0, increases monotonically)
        self._commit_index = 0

        # Index of highest log entry applied to state machine
        # (initialized to 0, increases monotonically)
        self._last_applied = 0

        # For each remote peer, index of the next log entry to send
        # (initialized to leader last log index + 1)
        self._next_index = {}

        # For each remote peer, index of the highest log entry known to be
        # replicated on that peer (initialized to 0, increases monotonically)
        self._match_index = {}

        # Latest term the server has seen
        # (initialized to 0 on first boot, increases monotonically).
        self._current_term = 0

    def is_leader(self):
        return self._current_state == ServerState._LEADER

    def is_candidate(self):
        return self._current_state == ServerState._CANDIDATE

    def is_follower(self):
        return self._current_state == ServerState._FOLLOWER

    def switch_to_leader(self):
        """Switch to leader state.

        Enable the heartbeat periodic call and
        stop to check if a leader is still alive.
        """

        if (not self._server.is_standalone() and
                self._current_state != ServerState._CANDIDATE):
            raise InvalidState(
                "Invalid state '%s' while transiting to leader state." %
                self._current_state)

        self._current_state = ServerState._LEADER
        self._voters.clear()
        self._voted_for = None
        LOG.info("switched to leader, term='%d'" % self._current_term)

        for remote_peer in self._server.remote_peers():
            self._next_index[remote_peer] = self._log.last_index() + 1
            self._match_index[remote_peer] = 0

        if not self._server.is_standalone():
            self._server.broadcast_append_entries()
            self._server.heartbeating.start()
        self._server.checking_leader_timeout.stop()

        if not self._server.is_standalone():
            command_id, noop_message = commands.build_no_operation()
            self._log.append_entry(self._current_term, noop_message)
            self._queued_commands[command_id] = (None, -1)

    def switch_to_follower(self, m_term, m_leader):
        """Switch to follower state.

        Disable the heartbeat periodic call and
        start to check if the leader is still alive.
        :param m_term: The last recent known term.
        :type m_term: int
        :param m_leader: The leader if a valid append entry has
        been received, None otherwise.
        :type: str
        """

        if self._current_state == ServerState._LEADER:
            self._server.checking_leader_timeout.start()
            self._server.heartbeating.stop()
        self._current_state = ServerState._FOLLOWER
        self._leader = m_leader
        self._current_term = max(m_term, self._current_term)
        self._voters.clear()
        self._voted_for = None
        LOG.info("switched to follower, term='%d'" % self._current_term)

    def switch_to_candidate(self):
        """Switch to candidate state.

        Increment the current term, vote for self, and broadcast a
        request vote. The election timeout is randomly reinitialized.
        """

        if self._current_state == ServerState._LEADER:
            raise InvalidState(
                "Invalid state '%s' while transiting to candidate state." %
                self._current_state)

        self._current_term += 1
        self._current_state = ServerState._CANDIDATE
        LOG.debug("switched to candidate, term='%d'" % self._current_term)
        self._voters.clear()
        self._voters.add(self._private_endpoint)
        self._voted_for = self._private_endpoint
        l_l_i, l_l_t = self._log.index_and_term_of_last_entry()
        rv_message = message.build_request_vote(self._current_term, l_l_i,
                                                l_l_t)

        # Broadcast request vote and reset election timeout.
        self._server.broadcast_message(rv_message)
        self._server.reset_election_timeout()

    def init_indexes(self, remote_peer_id):
        """Initialize next_index and match_index of a remote peer.

        :param remote_peer_id: The id of the remote peer.
        :type remote_peer_id: six.binary_type
        """
        self._next_index[remote_peer_id] = self._log.last_index() + 1
        self._match_index[remote_peer_id] = 0

    def next_index(self, peer_id):
        return self._next_index[peer_id]

    def update_next_index(self, peer_id, new_next_index):
        self._next_index[peer_id] = new_next_index

    def match_index_values(self):
        return self._match_index.values()

    def update_match_index(self, peer_id, new_match_index):
        self._match_index[peer_id] = max(self._match_index[peer_id],
                                         new_match_index)

    def commit_index(self):
        return self._commit_index

    def update_commit_index(self, new_commit_index):
        self._commit_index = new_commit_index

    def no_commands_to_apply(self):
        return self._last_applied == self._commit_index

    def last_applied(self):
        return self._last_applied

    def update_last_applied(self):
        self._last_applied = self._commit_index

    def clear(self):
        self._next_index.clear()
        self._match_index.clear()

    def term(self):
        return self._current_term

    def add_voter(self, peer_id):
        self._voters.add(peer_id)

    def grant_vote(self, peer_id):
        if not self._voted_for or self._voted_for == peer_id:
            self._voted_for = peer_id
            return True
        return False

    def number_of_voters(self):
        return len(self._voters)

    def update_leader(self, leader):
        self._leader = leader

    def is_leader_alive(self):
        return self._leader is not None


class InvalidState(Exception):
    """Exception raised when the server try to perform an action which
    is not allowed in its current state.
    """
