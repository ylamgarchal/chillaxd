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

import logging

import six
import zmq

LOG = logging.getLogger(__name__)


class Peer(object):
    """A remote peer which participate to RAFT protocol."""

    def __init__(self, context, local_identity, peer_identity):
        """
        :param context: An instance of zmq context.
        :type context: zmq.Context
        :param local_identity: The local address of the current server in the
        form of "address ip:port". It's used as an identifier for the remote
        peer.
        :type local_identity: str
        :param peer_identity: The address of the remote server in the form of
        "address ip:port".
        :type peer_identity: str
        """
        self._context = context
        self._zmq_dealer = None
        self._local_identity = local_identity
        self._peer_identity = peer_identity
        self._is_started = False

    def _setup(self):
        """Setup the attributes.

        Set the identity into the zmq DEALER socket.
        """
        self._zmq_dealer = self._context.socket(zmq.DEALER)
        self._zmq_dealer.setsockopt(zmq.IDENTITY, six.b(self._local_identity))
        self._zmq_dealer.setsockopt(zmq.LINGER, 0)

    def start(self):
        """Start the connection to the peer."""
        if not self._is_started:
            self._setup()
            self._zmq_dealer.connect("tcp://%s" % self._peer_identity)
            self._is_started = True
            LOG.debug("connecting to peer '%s'" % self._peer_identity)

    def stop(self):
        """Close the connection."""
        if self._is_started:
            self._zmq_dealer.close()
            self._is_started = False

    def send_message(self, message):
        """Send a message to the peer.

        :param message: The message to send.
        :type message: bytearray
        """
        if self._is_started:
            try:
                self._zmq_dealer.send(message, flags=zmq.NOBLOCK)
            except zmq.ZMQError:
                LOG.warn("send failed to peer '%s'" % self._peer_identity)
