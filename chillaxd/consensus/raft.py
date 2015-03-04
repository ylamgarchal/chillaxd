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

from __future__ import absolute_import

import logging
import random
import signal
import sys

import six
import zmq
from zmq.eventloop import ioloop

from . import peer
from chillaxd import commands
from chillaxd.consensus import log
from chillaxd.consensus import message
from chillaxd import datatree

LOG = logging.getLogger(__name__)


class Raft(object):
    """This class represents a server which implements the
    RAFT consensus protocol.
    """

    # Raft states.
    _LEADER = "LEADER"
    _CANDIDATE = "CANDIDATE"
    _FOLLOWER = "FOLLOWER"

    def __init__(self, private_endpoint, public_endpoint,
                 remote_endpoints=None, leader_heartbeat_interval=50,
                 min_election_timeout=200, max_election_timeout=300):
        """Init consensus server.

        :param private_endpoint: The private endpoint on which the
        server will bind to, in the form of "address ip:port".
        :type private_endpoint: str
        :param public_endpoint: The public endpoint on which the
        server will bind to, in the form of "address ip:port".
        :type public_endpoint: str
        :param remote_endpoints: A list of endpoints corresponding to
        the other peers.
        :type remote_endpoints: list
        :param leader_heartbeat_interval: The period of time between two
        heartbeat from the leader.
        :type leader_heartbeat_interval: int

        :param min_election_timeout: The minimum value for the election
        timeout.
        :type min_election_timeout: int
        :param max_election_timeout: The maximum value for the election
        timeout.
        :type max_election_timeout: int
        """
        super(Raft, self).__init__()

        self._private_endpoint = private_endpoint
        self._public_endpoint = public_endpoint
        self._remote_endpoints = remote_endpoints or []
        self._leader_heartbeat_interval = leader_heartbeat_interval
        self._min_election_timeout = min_election_timeout
        self._max_election_timeout = max_election_timeout

        self._remote_peers = {}

        # A quorum is a majority that is necessary for moving forward.
        self._quorum = int(((len(self._remote_endpoints) + 1) / 2)) + 1

        # A zmq timer that is activated when the server is not the leader
        # of the cluster. It periodically checks if the leader is alive with
        # a period randomly chosen between '_min_election_timeout' and
        # '_max_election_timeout' ms.
        self._checking_leader_timeout = None

        # A zmq timer that is activated when the server is the leader
        # of the cluster. It periodically send heartbeats to other peers within
        # a period defined by '_leader_heartbeat_interval' ms.
        self._heartbeating = None

        # The server state, initially a follower.
        self._state = Raft._FOLLOWER

        # The current known leader.
        self._leader = None

        # Set of peers that voted for this server in current term.
        self._voters = set()

        # The candidate that received vote in current term.
        self._voted_for = None

        # The replicated state machine is a tree.
        self._datatree = datatree.DataTree()

        # The log entries, each entry contains commands for the state machine.
        self._log = log.RaftLog()

        # Latest term the server has seen
        # (initialized to 0 on first boot, increases monotonically).
        self._current_term = 0

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

        # For each command id it associates the tuple (client id, response)
        # so that the server is able to send the response to the client. It
        # allows the server to handle the commands asynchronously through RAFT.
        self._queued_commands = {}

        # Zeromq context.
        self._context = None

        # Zeromq IO loop.
        self._zmq_ioloop = None

        # This socket is used for internal RAFT messages, the server will bind
        # with a zmq.ROUTER socket so that it will be fully asynchronous.
        self._socket_for_consensus = None

        # This socket is used for command messages, the server will bind with
        # a zmq.ROUTER socket so that it is fully asynchronous.
        self._socket_for_commands = None

        # Indicate if the server is started.
        self._is_started = False

    def _setup(self):
        """Setup all the attributes.

        Bind the server, connect to remote peers and initiate the timers.
        """

        self._context = zmq.Context()
        self._socket_for_commands = self._context.socket(zmq.ROUTER)
        self._socket_for_consensus = self._context.socket(zmq.ROUTER)
        for remote_endpoint in self._remote_endpoints:
            remote_peer = peer.Peer(self._context,
                                    self._private_endpoint,
                                    remote_endpoint)
            binary_r_e = six.b(remote_endpoint)
            self._remote_peers[binary_r_e] = remote_peer
            self._next_index[binary_r_e] = self._log.last_index() + 1
            self._match_index[binary_r_e] = 0

        self._zmq_ioloop = ioloop.ZMQIOLoop().instance()
        self._zmq_ioloop.add_handler(self._socket_for_commands,
                                     self._process_command_message,
                                     zmq.POLLIN)
        self._zmq_ioloop.add_handler(self._socket_for_consensus,
                                     self._dispatch_internal_raft_message,
                                     zmq.POLLIN)
        self._checking_leader_timeout = ioloop.PeriodicCallback(
            self._election_timeout_task,
            random.randint(self._min_election_timeout,
                           self._max_election_timeout),
            io_loop=self._zmq_ioloop)
        self._heartbeating = ioloop.PeriodicCallback(
            self._broadcast_ae_heartbeat,
            self._leader_heartbeat_interval,
            io_loop=self._zmq_ioloop)

    def _handle_signals(self, sig, frame):
        """Signal handler, stop gracefully the server."""

        self.stop()

    def _is_standalone(self):

        if not self._remote_endpoints:
            return True
        else:
            return False

    def start(self):
        """Start the server."""

        if not self._is_started:
            LOG.info("let's chillax on '%s'..." %
                     self._public_endpoint)
            # On SIGINT or SIGTERM signals stop and exit gracefully
            signal.signal(signal.SIGINT, self._handle_signals)
            signal.signal(signal.SIGTERM, self._handle_signals)
            self._setup()
            self._socket_for_consensus.bind("tcp://%s" %
                                            self._private_endpoint)
            self._socket_for_commands.bind("tcp://%s" %
                                           self._public_endpoint)
            if not self._is_standalone():
                for remote_server in six.itervalues(self._remote_peers):
                    remote_server.start()
                self._checking_leader_timeout.start()
            else:
                self._switch_to_leader()

            self._is_started = True
            self._zmq_ioloop.start()

    def stop(self):
        """Stop gracefully the server."""

        if self._is_started:
            self._checking_leader_timeout.stop()
            self._heartbeating.stop()
            self._socket_for_commands.close()
            self._socket_for_consensus.close()
            for remote_server in six.itervalues(self._remote_peers):
                remote_server.stop()
            self._zmq_ioloop.stop()
            self._context.destroy(linger=0)
            self._is_started = False
            self._next_index.clear()
            self._match_index.clear()
            LOG.info("chillaxd stopped")

    # TODO(yassine): if not the leader then send leader hint to the client
    def _process_command_message(self, socket, event):
        """Processes a command from a client.

        In case of a read command the server respond immediately to the client
        otherwise it just add the command in the log so that it will be
        replicated.

        :param socket: The zmq.ROUTER socket.
        :type socket: zmq.sugar.socket.Socket
        :param event: The corresponding event, it should only be zmq.POLLIN.
        :type event: int
        """

        zmq_message = socket.recv_multipart()
        client_identifier, command = zmq_message[0], zmq_message[1]

        command_type, command_id, payload = commands.decode_command(command)

        # If it is a read command then just send immediately the result.
        if commands.is_read_command(command_type):
            try:
                data = self._datatree.apply_command(command_type, *payload)
            except datatree.FsmException as e:
                response = commands.build_response(command_type, command_id,
                                                   e.errno)
            else:
                response = commands.build_response(command_type, command_id, 0,
                                                   data)
            socket.send_multipart((client_identifier, response))
        else:
            # If it is a write command then:
            #     1. add the command to the commands queue
            #     2. append it to the log, it will be piggybacked
            #        with the next heartbeat
            self._queued_commands[command_id] = (client_identifier, -1)
            self._log.append_entry(self._current_term, command)
            if self._is_standalone():
                # If it's a standalone server then we can directly commit
                # the command.
                self._commit_index += 1
                self._apply_committed_log_entries_to_state_machine()
                self._send_write_responses(max(1, self._commit_index - 1))

    def _dispatch_internal_raft_message(self, socket, event):
        """Decode the received message and dispatch it on the corresponding
        handler.

        :param socket: The zmq.ROUTER socket.
        :type socket: zmq.sugar.socket.Socket
        :param event: The corresponding event, it should only be zmq.POLLIN.
        :type event: int
        """

        assert event == zmq.POLLIN
        zmq_message = socket.recv_multipart()

        # TODO(yassine): check m_identitifer is a known server.
        identifier, payload = zmq_message

        message_type, params = message.decode_message(payload)

        if message_type == message.APPEND_ENTRY_REQUEST:
            self._process_append_entry_request(identifier, *params)
        elif message_type == message.APPEND_ENTRY_RESPONSE:
            self._process_append_entry_response(identifier, *params)
        elif message_type == message.REQUEST_VOTE:
            self._process_request_vote(identifier, *params)
        elif message_type == message.REQUEST_VOTE_RESPONSE:
            self._process_request_vote_response(identifier, *params)
        else:
            # TODO(yassine): add dict to translate int to human readable type
            LOG.error("unknown message type '%s'" % message_type)

    def _process_append_entry_request(self, m_leader_id, remote_term,
                                      leader_prev_log_index,
                                      leader_prev_log_term,
                                      leader_commit_index,
                                      leader_entries):
        """Processes the append entry request.

        :param m_leader_id: The identifier of the remote peer in the form of
        "address ip:port".
        :type: str
        :param remote_term: The term of the leader.
        :type remote_term: int
        :param leader_prev_log_index: The previous log entry of the leader.
        :type leader_prev_log_index: int
        :param leader_prev_log_term: The previous log term of the leader.
        :type leader_prev_log_term: int
        :param leader_commit_index: The commit index of the leader.
        :type leader_commit_index: int
        :param leader_entries: The leader_entries to add next to the previous
        log entry of the leader.
        :type leader_entries: tuple
        """

        # Received a stale request then respond negatively.
        if self._current_term > remote_term:
            LOG.debug("stale append entry from '%s'" % m_leader_id)
            ae_response_ko = message.build_append_entry_response(
                self._current_term, False, None)
            self._remote_peers[m_leader_id].send_message(ae_response_ko)
        # The current server is outdated then switch to follower.
        elif self._current_term < remote_term:
            self._switch_to_follower(remote_term, m_leader_id)
            ae_response_ko = message.build_append_entry_response(
                self._current_term, False, None)
            self._remote_peers[m_leader_id].send_message(ae_response_ko)
        else:
            if self._state == Raft._LEADER:
                LOG.error("'%s' elected at same term '%d'" %
                          (m_leader_id, remote_term))
                self._switch_to_follower(remote_term, None)
                return
            LOG.debug("leader='%s', term='%d'" % (m_leader_id,
                                                  self._current_term))
            # If the peer is in candidate state and received an append entry
            # request in the same term, it means the remote peer has been
            # elected, then switch to follower state.
            if self._state == Raft._CANDIDATE:
                self._switch_to_follower(remote_term, m_leader_id)
            # The leader is alive.
            self._leader = m_leader_id

            local_prev_log_index = self._log.entry_at_index(
                leader_prev_log_index, decode=True)
            local_entry_term = local_prev_log_index[1]
            # If induction checking is verified then add the leader_entries to
            # the log and send positive response otherwise respond negatively.
            if local_entry_term == leader_prev_log_term:
                LOG.info("received append entry request, induction checking "
                         "succeed, previous_entry_index='%s', "
                         "previous_entry_term='%s'" % (leader_prev_log_index,
                                                       leader_prev_log_term))

                self._log.add_entries_at_start_index(leader_prev_log_index + 1,
                                                     leader_entries)
                last_log_index = self._log.last_index()
                ae_response_ok = message.build_append_entry_response(
                    self._current_term, True, last_log_index)
                self._remote_peers[m_leader_id].send_message(ae_response_ok)

                # Update local commit_index.
                if leader_commit_index > self._commit_index:
                    self._commit_index = min(leader_commit_index,
                                             self._log.last_index())

                # Check if leader entries  are committed and apply them to
                # the state machine if they are not applied yet.
                self._apply_committed_log_entries_to_state_machine()
            else:
                LOG.warn("received append entry request, induction checking "
                         "failed, local entry term='%s', leader entry "
                         "term='%s'" % (local_entry_term,
                                        leader_prev_log_term))
                ae_response_ko = message.build_append_entry_response(
                    self._current_term, False, None)
                self._remote_peers[m_leader_id].send_message(ae_response_ko)

    def _send_write_responses(self, first_non_ack_command_index):
        """Send acknowledgments for write requests.

        Once the leader committed commands it sends the acknowledgments
        to the corresponding client.

        :param first_non_ack_command_index: the first command index which the
        server didn't send an acknowledgment.
        :type first_non_ack_command_index: int
        """
        for index in six.moves.range(first_non_ack_command_index,
                                     self._commit_index + 1):
            command = self._log.entry_at_index(index, decode=True)[2]
            command_type, command_id, payload = commands.decode_command(
                command)

            if command_type == commands.NO_OPERATION:
                continue

            client_id, status = self._queued_commands.get(command_id)
            if client_id:
                response = commands.build_response(command_type, command_id,
                                                   status)
                self._socket_for_commands.send_multipart((client_id, response))
                del self._queued_commands[command_id]

    def _process_append_entry_response(self, follower_id, follower_term,
                                       success, follower_last_log_index):
        """Processes the append entry response.

        :param follower_id: The identifier of the remote peer in the form of
        "address ip:port".
        :type: str
        :param follower_term: The term of the follower.
        :type follower_term: int
        :param success: Indicates if the append entry request succeed.
        :type success: bool
        :param follower_last_log_index: The last log replicated by the
        follower.
        :type follower_last_log_index: int
        """

        # If it is not in leader state then it doesn't care about append
        # entry responses.
        if self._state != self._LEADER:
            return

        # Received a stale request then ignore it.
        if self._current_term > follower_term:
            LOG.debug("stale append entry from '%s'" % follower_id)
        # The current server is outdated then switch to follower.
        elif self._current_term < follower_term:
            LOG.debug("server outdated, will switch to follower")
            self._switch_to_follower(follower_term, None)
        else:
            if success:
                # If the append entry request succeed then we update the match
                # index of that follower and see if some entries are committed.
                self._match_index[follower_id] = max(
                    self._match_index[follower_id], follower_last_log_index)
                self._next_index[follower_id] = min(
                    follower_last_log_index + 1, self._log.last_index() + 1)

                # Some magic here
                all_match_index = list(self._match_index.values())
                all_match_index.append(self._log.last_index())
                all_match_index.sort(reverse=True)
                majority_index = int(len(all_match_index) / 2)
                committed_index = all_match_index[majority_index]
                committed_term = self._log.entry_at_index(committed_index,
                                                          decode=True)[1]
                if self._current_term == committed_term:
                    first_non_ack_command_index = self._commit_index + 1
                    self._commit_index = max(committed_index,
                                             self._commit_index)
                    self._apply_committed_log_entries_to_state_machine()
                    self._send_write_responses(first_non_ack_command_index)
            else:
                # TODO(yassine): this is naive, make it faster.
                # In case the induction checking failed on the follower then
                # decrement its next index.
                self._next_index[follower_id] = max(
                    1, self._next_index[follower_id] - 1)

    def _is_candidate_log_up_to_date(self, candidate_log_index,
                                     candidate_log_term):
        """Tells if the candidate's log is up to date compared to the local
        logs.

        :param candidate_log_index: Candidate's log index.
        :type candidate_log_index: int
        :param candidate_log_term: Candidate's log term.
        :type candidate_log_term: int
        :return: A boolean which indicate if the log is up to date.
        :rtype: bool
        """

        local_l_l_i, local_l_l_t = self._log.index_and_term_of_last_entry()
        if local_l_l_t < candidate_log_term:
            return True
        elif local_l_l_t > candidate_log_term:
            return False
        else:
            return local_l_l_i <= candidate_log_index

    def _process_request_vote(self, candidate_id, candidate_term,
                              last_log_index, last_log_term):
        """Processes the request vote request.

        :param candidate_id: The identifier of the remote peer in the form of
        "address ip:port".
        :type candidate_id: str
        :param candidate_term: Term of the candidate peer.
        :type candidate_term: int
        :param last_log_index: Last log index of the candidate peer.
        :type last_log_index: int
        :param last_log_term: Last log term of the candidate peer.
        :type last_log_term: int
        """

        # Received a stale request then respond negatively.
        if self._current_term > candidate_term:
            LOG.debug("request vote denied to '%s', stale term" % candidate_id)
            rv_response_ko = message.build_request_vote_response(
                self._current_term, False)
            self._remote_peers[candidate_id].send_message(rv_response_ko)
            return

        # The current server is outdated then switch to follower.
        if self._current_term < candidate_term:
            LOG.debug("server outdated, switch to follower")
            self._switch_to_follower(candidate_term, None)

        vote = False
        if self._is_candidate_log_up_to_date(last_log_index, last_log_term):
            if not self._voted_for or self._voted_for == candidate_id:
                vote = True
                self._voted_for = candidate_id

        LOG.debug("send request vote response '%s' to '%s" % (vote,
                                                              candidate_id))
        rv_response = message.build_request_vote_response(
            self._current_term, vote)
        self._remote_peers[candidate_id].send_message(rv_response)

    def _process_request_vote_response(self, m_identifier,
                                       remote_term,
                                       vote_granted):
        """Processes the request vote response.

        :param m_identifier: The identifier of the remote peer in the form of
        "address ip:port".
        :type: str
        :param remote_term: The term of the remote peer.
        :type remote_term: int
        :param vote_granted: The vote response of the remote peer.
        :type vote_granted: bool
        """

        # If it is not in candidate state then it doesn't care about append
        # request vote responses.
        if self._state != self._CANDIDATE:
            return

        if self._current_term > remote_term:
            LOG.debug("request vote response from '%s' ignored, stale term" %
                      m_identifier)
        elif self._current_term < remote_term:
            LOG.debug("request vote denied from '%s'" % m_identifier)
            self._switch_to_follower(remote_term, None)
        else:
            if vote_granted:
                self._voters.add(m_identifier)
                if len(self._voters) >= self._quorum:
                    self._switch_to_leader()
            else:
                LOG.debug("request vote denied from '%s'" % m_identifier)

    def _queue_command_if_leader(self, command_id, client_id, errno):
        """Adds a command to the queue commands if in leader state."""
        if self._state == Raft._LEADER:
            self._queued_commands[command_id] = (client_id, errno)

    def _apply_committed_log_entries_to_state_machine(self):
        """Apply committed log entries to the state machine."""

        # if there is no new commands to apply then exit from the function
        if self._last_applied == self._commit_index:
            return

        for index in six.moves.range(self._last_applied + 1,
                                     self._commit_index + 1):
            _, _, payload = self._log.entry_at_index(index, decode=True)
            command_type, command_id, payload = commands.decode_command(
                payload)
            client_id = self._queued_commands.get(command_id, (-1, -1))[0]

            try:
                self._datatree.apply_command(command_type, *payload)
            except datatree.FsmException as e:
                self._queue_command_if_leader(command_id, client_id, e.errno)
            else:
                self._queue_command_if_leader(command_id, client_id, 0)

        self._last_applied = self._commit_index
        LOG.info("commit_index='%s', last_applied='%s'" %
                 (self._commit_index, self._last_applied))

    def _broadcast_ae_heartbeat(self):
        """Broadcast append entries to all peers.

        This method is periodically called by zeromq timer within a period
        equals to '_leader_heartbeat_interval' ms.
        """

        if self._state != Raft._LEADER:
            raise InvalidState(
                "Invalid state '%d' while sending heartbeat.")
            sys.exit(1)
        LOG.info("send append entry heartbeat, term='%d'" % self._current_term)

        # Broadcast an append entry request.
        for remote_server_id in six.iterkeys(self._remote_peers):
            next_index = self._next_index[remote_server_id]
            p_l_i, p_l_t = self._log.prev_index_and_term_of_entry(next_index)
            entries = self._log.entries_from_index(next_index)
            ae_request = message.build_append_entry_request(self._current_term,
                                                            p_l_i, p_l_t,
                                                            self._commit_index,
                                                            entries)
            self._remote_peers[remote_server_id].send_message(ae_request)

    def _election_timeout_task(self):
        """Check periodically if the leader is still alive.

        If there is no peers, the cluster is composed of only one
        node then switch safely to leader. If the leader had not sent
        heartbeats then switch to candidate state.
        """

        if self._state == Raft._LEADER:
            raise InvalidState(
                "Invalid state '%d' while checking election timeout.")
            sys.exit(1)

        if not self._leader:
            if not len(self._remote_endpoints):
                self._switch_to_leader()
                return
            self._switch_to_candidate()
        self._leader = None

    def _switch_to_leader(self):
        """Switch to leader state.

        Enable the heartbeat periodic call and
        stop to check if the leader is still alive.
        """

        if not self._is_standalone() and self._state != Raft._CANDIDATE:
            raise InvalidState(
                "Invalid state '%s' while transiting to leader state." %
                self._state)
            sys.exit(1)
        self._state = Raft._LEADER
        self._voters.clear()
        self._voted_for = None

        for remote_peer in self._remote_peers:
            self._next_index[remote_peer] = self._log.last_index() + 1
            self._match_index[remote_peer] = 0

        if not self._is_standalone():
            self._broadcast_ae_heartbeat()
            self._heartbeating.start()
        self._checking_leader_timeout.stop()

        if not self._is_standalone():
            command_id, noop_message = commands.build_no_operation()
            self._log.append_entry(self._current_term, noop_message)
            self._queued_commands[command_id] = (-1, -1)

        LOG.info("switched to leader, term='%d'" % self._current_term)

    def _switch_to_follower(self, m_term, m_leader):
        """Switch to follower state.

        Disable the heartbeat periodic call and
        start to check if the leader is still alive.
        :param m_term: The last recent known term.
        :type m_term: int
        :param m_leader: The leader if a valid append entry has
        been received, None otherwise.
        :type: str
        """

        if self._state == Raft._LEADER:
            self._checking_leader_timeout.start()
            self._heartbeating.stop()
        self._state = Raft._FOLLOWER
        self._leader = m_leader
        self._current_term = max(m_term, self._current_term)
        self._voters.clear()
        self._voted_for = None
        LOG.info("switched to follower, term='%d'" % self._current_term)

    def _switch_to_candidate(self):
        """Switch to candidate state.

        Increment the current term, vote for self, and broadcast a
        request vote. The election timeout is randomly reinitialized.
        """

        if self._state == Raft._LEADER:
            raise InvalidState(
                "Invalid state '%s' while transiting to candidate state." %
                self._state)
            sys.exit(1)
        self._current_term += 1
        self._state = Raft._CANDIDATE
        LOG.debug("switched to candidate, term='%d'" % self._current_term)
        self._voters.clear()
        self._voters.add(self._private_endpoint)
        self._voted_for = self._private_endpoint
        l_l_i, l_l_t = self._log.index_and_term_of_last_entry()
        rv_message = message.build_request_vote(self._current_term, l_l_i,
                                                l_l_t)
        # Broadcast request vote.
        for remote_server in six.itervalues(self._remote_peers):
            remote_server.send_message(rv_message)
        new_election_timeout = random.randint(self._min_election_timeout,
                                              self._max_election_timeout)
        self._checking_leader_timeout.callback_time = new_election_timeout
        LOG.debug("new election timeout '%d'ms" % new_election_timeout)


class InvalidState(Exception):
    """Exception raised when the server try to perform an action which
    is not allowed in its current state.
    """
