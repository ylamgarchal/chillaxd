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

from __future__ import absolute_import

import logging
import random
import signal

import six
import zmq
from zmq.eventloop import ioloop

from . import peer
from chillaxd import command
from chillaxd.consensus import log
from chillaxd.consensus import message
from chillaxd import datatree

LOG = logging.getLogger(__name__)

_HEARTBEAT_INTERVAL = 10
_MIN_ELECTION_TIMEOUT = 15
_MAX_ELECTION_TIMEOUT = 20


class Raft(object):
    """This class represents a server which implements the
    RAFT consensus protocol."""

    # Raft states.
    LEADER = 1
    CANDIDATE = 2
    FOLLOWER = 3

    def __init__(self, local_server_endpoint, remote_server_endpoints=set()):
        """Init consensus server.

        :param local_server_endpoint: The endpoint on which the server will
        bind to, in the form of "address ip:port".
        :type local_server_endpoint: str
        :param remote_server_endpoints: A set of endpoints corresponding to
        the other peers.
        :type remote_server_endpoints: set
        """
        super(Raft, self).__init__()

        self._local_server_endpoint = local_server_endpoint
        self._remote_server_endpoints = remote_server_endpoints
        self._remote_servers = {}

        # A quorum is a majority that are necessary for moving forward.
        self._quorum = int(((len(self._remote_server_endpoints) + 1) / 2)) + 1

        # A zmq timer that is activated when the server is not the leader
        # of the cluster. It periodically checks if the leader is alive with
        # a period randomly chosen between _MIN_ELECTION_TIMEOUT and
        # _MAX_ELECTION_TIMEOUT ms.
        self._check_leader_timeout = None

        # A zmq timer that is activated when the server is the leader
        # of the cluster. It periodically send heartbeats to other peers with
        # a period defined by _HEARTBEAT_INTERVAL ms.
        self._heartbeating = None

        # The server start as a follower.
        self._state = Raft.FOLLOWER

        # The leader is not elected yet.
        self._leader = None

        # Set of peers that voting for this server in current term.
        self._voters = set()

        # Candidate that received vote in current term.
        self._voted_for = None

        # TODO(yassine): apply logs to the datatree
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

        # Zmq context.
        self._context = None

        # Zmq IO loop.
        self._zmq_ioloop = None

        # This socket is used for internal RAFT messages, the server will bind
        # with a zmq.ROUTER socket so that it is fully asynchronous.
        self._socket_for_consensus = None

        # This socket is used for command messages, the server will bind with
        # a zmq.ROUTER socket so that it is fully asynchronous.
        self._socket_for_commands = None
        self._is_started = False

    def _setup(self):
        """Set the attributes.

        Bind the server, connect to remote peers and
        initiate the timers."""
        self._context = zmq.Context()
        self._socket_for_commands = self._context.socket(zmq.ROUTER)
        self._socket_for_consensus = self._context.socket(zmq.ROUTER)
        for remote_server_endpoint in self._remote_server_endpoints:
            remote_server = peer.Peer(self._context,
                                      self._local_server_endpoint,
                                      remote_server_endpoint)
            self._remote_servers[six.b(remote_server_endpoint)] = remote_server

        self._zmq_ioloop = ioloop.ZMQIOLoop().instance()
        self._zmq_ioloop.add_handler(self._socket_for_commands,
                                     self._process_command_message,
                                     zmq.POLLIN)
        self._zmq_ioloop.add_handler(self._socket_for_consensus,
                                     self._dispatch_internal_raft_message,
                                     zmq.POLLIN)
        self._check_leader_timeout = ioloop.PeriodicCallback(
            self._election_timeout_task,
            random.randint(_MIN_ELECTION_TIMEOUT, _MAX_ELECTION_TIMEOUT),
            io_loop=self._zmq_ioloop)
        self._heartbeating = ioloop.PeriodicCallback(self._send_heartbeat,
                                                     _HEARTBEAT_INTERVAL,
                                                     io_loop=self._zmq_ioloop)

    def _handle_signals(self, sig, frame):
        """Signal handler, stop gracefully the server."""
        self.stop()

    def start(self):
        """Start the server."""
        if not self._is_started:
            LOG.info("let's chillax on '%s'..." %
                     self._local_server_endpoint)
            # On SIGINT or SIGTERM signals stop and exit gracefully
            signal.signal(signal.SIGINT, self._handle_signals)
            signal.signal(signal.SIGTERM, self._handle_signals)
            self._setup()
            self._socket_for_consensus.bind("tcp://%s" %
                                            self._local_server_endpoint)
            self._socket_for_commands.bind("ipc://%s" %
                                           self._local_server_endpoint)
            for remote_server in six.itervalues(self._remote_servers):
                remote_server.start()
            self._is_started = True
            self._check_leader_timeout.start()
            self._zmq_ioloop.start()

    def stop(self):
        """Stop gracefully the server."""
        if self._is_started:
            self._check_leader_timeout.stop()
            self._heartbeating.stop()
            self._socket_for_consensus.close()
            for remote_server in six.itervalues(self._remote_servers):
                remote_server.stop()
            self._zmq_ioloop.stop()
            self._context.destroy(linger=0)
            self._is_started = False
            LOG.info("chillaxd stopped")

    def _process_command_message(self, socket, event):
        """Processes a command from a client.

        :param socket: The zmq.ROUTER socket.
        :type socket: zmq.sugar.socket.Socket
        :param event: The corresponding event, it should only be zmq.POLLIN.
        :type event: int
        """
        zmq_command = socket.recv_multipart()
        m_identifier, m_command = zmq_command[0], zmq_command[1]

        decoded_command = command.decode_command(m_command)

        if decoded_command[0] == command.GET_CHILDREN:
            children = self._datatree.get_children(decoded_command[1])
            socket.send_multipart((m_identifier, "", children))
            return

        # Add a new log entry for the command.
        self._log.append_entry(self._current_term, m_command)

        # Broadcast an append entry request.
        entries = [self._log.last_entry()]
        p_l_i, p_l_t = self._log.index_and_term_from_prev_last_entry()
        ae_request = message.build_append_entry(self._current_term, p_l_i,
                                                p_l_t, entries)
        self._broadcast_message(ae_request)

    def _dispatch_internal_raft_message(self, socket, event):
        """Decode the received message and dispatch on the corresponding
        handler.

        :param socket: The zmq.ROUTER socket.
        :type socket: zmq.sugar.socket.Socket
        :param event: The corresponding event, it should only be zmq.POLLIN.
        :type event: int
        """
        assert event == zmq.POLLIN
        zmq_message = socket.recv_multipart()
        m_identifier, payload = zmq_message

        if self._state == Raft.FOLLOWER:
            self._handle_as_follower(m_identifier, payload)
        elif self._state == Raft.LEADER:
            self._handle_as_leader(m_identifier, payload)
        elif self._state == Raft.CANDIDATE:
            self._handle_as_candidate(m_identifier, payload)
        else:
            LOG.critical("unknown state")

    def _handle_as_leader(self, m_identifier, m_payload):
        """Handle message as a leader.

        :param m_identifier: The identifier of the remote peer in the form of
        "address ip:port".
        :type m_identifier: str
        :param m_payload: The message payload.
        :type m_payload: six.binary_type
        """

        m_type, params = self._decode_message_payload(m_payload)

        if m_type == message.APPEND_ENTRY_REQUEST:
            self._process_append_entry_request(m_identifier, *params)
        elif m_type == message.APPEND_ENTRY_RESPONSE:
            self._process_append_entry_response(m_identifier, *params)
        elif m_type == message.REQUEST_VOTE:
            self._process_request_vote(m_identifier, *params)
        else:
            LOG.debug("message type '%s' ignored" % m_type)

    def _handle_as_follower(self, m_identifier, m_payload):
        """Handle message as a follower.

        :param m_identifier: The identifier of the remote peer in the form of
        "address ip:port".
        :type m_identifier: str
        :param m_payload: The message payload.
        :type m_payload: six.binary_type
        """

        m_type, params = self._decode_message_payload(m_payload)

        if m_type == message.APPEND_ENTRY_REQUEST:
            self._process_append_entry_request(m_identifier, *params)
        elif m_type == message.REQUEST_VOTE:
            self._process_request_vote(m_identifier, *params)
        else:
            LOG.debug("message type '%s' ignored" % m_type)

    def _handle_as_candidate(self, m_identifier, m_payload):
        """Handle message as a candidate.

        :param m_identifier: The identifier of the remote peer in the form of
        "address ip:port".
        :type m_identifier: str
        :param m_payload: The message payload.
        :type m_payload: six.binary_type
        """

        m_type, params = self._decode_message_payload(m_payload)

        if m_type == message.APPEND_ENTRY_REQUEST:
            self._process_append_entry_request(m_identifier, *params)
        elif m_type == message.REQUEST_VOTE:
            self._process_request_vote(m_identifier, *params)
        elif m_type == message.REQUEST_VOTE_RESPONSE:
            self._process_request_vote_response(m_identifier, *params)
        else:
            LOG.debug("message type '%s' ignored" % m_type)

    def _process_append_entry_request(self, m_identifier, term, prev_log_index,
                                      prev_log_term, leader_commit, entries):
        """Processes the append entries request.

        :param m_identifier: The identifier of the remote peer in the form of
        "address ip:port".
        :type: str
        :param term: The term of the leader.
        :type term: int
        :param prev_log_index: The previous log entry of the leader.
        :type prev_log_index: int
        :param prev_log_term: The previous log term of the leader.
        :type prev_log_term: int
        :param leader_commit: The commit index of the leader.
        :type leader_commit: int
        :param entries: The entries to add next to the previous log entry of
        the leader.
        :type entries: tuple
        """

        # Received a stale request then respond negatively.
        if self._current_term > term:
            LOG.debug("stale append entry from '%s'" % m_identifier)
            ae_response = message.build_append_entry_response(
                self._current_term, False)
            self._remote_servers[m_identifier].send_message(ae_response)
        # The current server is outdated then switch to follower.
        elif self._current_term < term:
            self._switch_to_follower(term, m_identifier)
            ae_response = message.build_append_entry_response(
                self._current_term, False)
            self._remote_servers[m_identifier].send_message(ae_response)
        elif self._state == Raft.LEADER:
            LOG.error("'%s' elected at same term '%d'" %
                      (m_identifier, term))
            self._switch_to_follower(term, None)
        else:
            LOG.debug("leader='%s', term='%d'" % (m_identifier,
                                                  self._current_term))
            # If we received an append entry in the same term it means the
            # remote peer has been elected, so switch to follower.
            if self._state == Raft.CANDIDATE:
                self._switch_to_follower(term, m_identifier)
            # The leader is alive.
            self._leader = m_identifier

            entry_at_prev_log_index = self._log.entry_at_index(prev_log_index,
                                                               decode=True)
            entry_term = entry_at_prev_log_index[1]
            # If induction checking is verified then add the entries to the
            # log and send positive response otherwise respond negatively.
            if entry_term == prev_log_term:
                LOG.info("received append entry request, induction checking "
                         "succeed, local entry term='%s'" % entry_term)

                self._log.add_entries_at_start_index(prev_log_index + 1,
                                                     entries)
                ae_response = message.build_append_entry_response(
                    self._current_term, True)
                self._remote_servers[m_identifier].send_message(ae_response)

                # Update local commit_index according to RAFT.
                if leader_commit > self._commit_index:
                    self._commit_index = min(leader_commit,
                                             self._log.last_index())

                # Check if entries  are committed and apply them to
                # the state machine if they are not applied yet.
                self._apply_committed_log_entries_to_state_machine()
            else:
                LOG.warn("received append entry request, induction checking "
                         "failed, local entry term='%s', leader entry "
                         "term='%s'" % (entry_term, prev_log_term))
                ae_response = message.build_append_entry_response(
                    self._current_term, True)
                self._remote_servers[m_identifier].send_message(ae_response)

    def _apply_committed_log_entries_to_state_machine(self):
        """Apply committed log entries to the state machine."""

        while self._commit_index > self._last_applied:
            self._last_applied += 1
            log_entry = self._log.entry_at_index(self._last_applied)
            decoded_entry = log.decode_log_entry(log_entry)

            if decoded_entry[0] == command.CREATE_NODE:
                self._datatree.create_node(decoded_entry[1], decoded_entry[2])
            else:
                print("wtf dude ?")

    def _process_append_entry_response(self, m_identifier, m_term, m_payload):
        """Processes the append entries response.

        :param m_identifier: The identifier of the remote peer in the form of
        "address ip:port".
        :type: str
        :param m_term: The term of the follower.
        :type m_term: int
        :param m_payload: The message payload.
        :type m_payload: six.binary_type
        """

    def _process_request_vote(self, m_identifier, term, last_log_index,
                              last_log_term):
        """Processes the request vote request.

        :param m_identifier: The identifier of the remote peer in the form of
        "address ip:port".
        :type: str
        :param term: Term of the candidate peer.
        :param term: int
        :param last_log_index: Last log index of the candidate peer.
        :type last_log_index: int
        :param last_log_term: Last log term of the candidate peer.
        :type last_log_term: int
        """

        # Received a stale request then respond negatively.
        if self._current_term > term:
            LOG.debug("request vote denied to '%s', stale term" % m_identifier)
            rv_response = message.build_request_vote_response(
                self._current_term, False)
            self._remote_servers[m_identifier].send_message(rv_response)
        # The current server is outdated then switch to follower.
        elif self._current_term < term:
            self._switch_to_follower(term, None)
            LOG.debug("request vote granted to '%s'" % m_identifier)
            rv_response = message.build_request_vote_response(
                self._current_term, True)
            self._remote_servers[m_identifier].send_message(rv_response)
            self._voted_for = m_identifier
        else:
            # If we received the request in the current term and we had not
            # yet voted then vote for this candidate otherwise deny.
            if not self._voted_for:
                LOG.debug("request vote granted to '%s'" % m_identifier)
                rv_response = message.build_request_vote_response(
                    self._current_term, True)
                self._remote_servers[m_identifier].send_message(rv_response)
                self._voted_for = m_identifier
            else:
                LOG.debug("request vote denied to '%s'" % m_identifier)
                rv_response = message.build_request_vote_response(
                    self._current_term, False)
                self._remote_servers[m_identifier].send_message(rv_response)

    def _process_request_vote_response(self, m_identifier, term, vote_granted):
        """Processes the request vote response.

        :param m_identifier: The identifier of the remote peer in the form of
        "address ip:port".
        :type: str
        :param term: The term of the remote peer.
        :type term: int
        :param vote_granted: The vote response of the remote peer.
        :type vote_granted: bool
        """
        if self._current_term > term:
            LOG.debug("request vote response from '%s' ignored, stale term" %
                      m_identifier)
        elif self._current_term < term:
            LOG.debug("request vote denied from '%s'" % m_identifier)
            self._switch_to_follower(term, None)
        else:
            if vote_granted:
                self._voters.add(m_identifier)
                if len(self._voters) >= self._quorum:
                    self._switch_to_leader()
            else:
                LOG.debug("request vote denied from '%s'" % m_identifier)

    @staticmethod
    def _decode_message_payload(m_payload):
        """Decode a message payload.

        :param m_payload: The message payload.
        :type m_payload: six.binary_type
        :return: The decoded message payload in the form of
        (MESSAGE TYPE, arguments)
        """
        decoded_message = message.decode_message(m_payload)
        return decoded_message[0], decoded_message[1:]

    def _send_heartbeat(self):
        """Send heartbeats to all peers.

        This method is periodically called by zmq timer with a period
        equals to raft._HEARTBEAT_INTERVAL ms.
        """
        if self._state != Raft.LEADER:
            raise InvalidState(
                "Invalid state '%d' while sending heartbeat.")
        LOG.info("send append entry heartbeat, term='%d'" % self._current_term)

        # Broadcast an append entry request.
        p_l_i, p_l_t = self._log.index_and_term_from_prev_last_entry()
        ae_request = message.build_append_entry_request(self._current_term,
                                                        p_l_i, p_l_t,
                                                        self._commit_index, ())
        self._broadcast_message(ae_request)

    def _election_timeout_task(self):
        """Check periodically if the leader is still alive.

        If there is no peers, the cluster is composed of only one
        node then switch safely to leader. If the leader had not sent
        heartbeats then switch to candidate state.
        """
        if self._state == Raft.LEADER:
            raise InvalidState(
                "Invalid state '%d' while checking election timeout.")

        if not self._leader:
            if not len(self._remote_server_endpoints):
                self._switch_to_leader()
                return
            self._switch_to_candidate()
        self._leader = None

    def _broadcast_message(self, zmq_message):
        """Utility method to broadcast a message to all peers.

        :param zmq_message: The message to broadcast.
        :type zmq_message: six.binary_type
        """
        for remote_server in six.itervalues(self._remote_servers):
            remote_server.send_message(zmq_message)

    def _switch_to_leader(self):
        """Switch to leader state.

        Enable the heartbeat periodic call and
        stop to check if the leader is still alive.
        """
        if self._state != Raft.CANDIDATE:
            raise InvalidState(
                "Invalid state '%d' while transiting to leader state." %
                self._state)
        self._state = Raft.LEADER
        self._voters.clear()
        self._voted_for = None
        if len(self._remote_server_endpoints):
            self._send_heartbeat()
            self._heartbeating.start()
        self._check_leader_timeout.stop()
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
        if self._state == Raft.LEADER:
            self._check_leader_timeout.start()
            self._heartbeating.stop()
        self._state = Raft.FOLLOWER
        self._leader = m_leader
        self._current_term = max(m_term, self._current_term)
        self._voters.clear()
        self._voted_for = None
        LOG.info("switched to follower, term='%d'" % self._current_term)

    def _switch_to_candidate(self):
        """Switch to candidate state.

        Increment the current term, vote for self, and broadcast a
        request vote. The election timeout is randomly reinitialized
        according to RAFT protocol.
        """
        if self._state != Raft.CANDIDATE and self._state != Raft.FOLLOWER:
            raise InvalidState(
                "Invalid state '%d' while transiting to candidate state." %
                self._state)
        self._current_term += 1
        self._state = Raft.CANDIDATE
        LOG.debug("switched to candidate, term='%d'" % self._current_term)
        self._voters.clear()
        self._voters.add(self._local_server_endpoint)
        self._voted_for = self._local_server_endpoint
        l_l_i, l_l_t = self._log.index_and_term_from_last_entry()
        rv_message = message.build_request_vote(self._current_term, l_l_i,
                                                l_l_t)
        self._broadcast_message(rv_message)
        new_election_timeout = random.randint(_MIN_ELECTION_TIMEOUT,
                                              _MAX_ELECTION_TIMEOUT)
        self._check_leader_timeout.callback_time = new_election_timeout
        LOG.debug("new election timeout '%d'ms" % new_election_timeout)


class InvalidState(Exception):
    """Exception raised when the server try to perform an action which
    is not defined in its current state."""
