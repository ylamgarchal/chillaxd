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
import six
import zmq

from chillaxd.raft import peer


class TestPeer(object):

    def test_init(self):
        testPeer = peer.Peer(zmq.Context.instance(), "local_identity",
                             "peer_identity")
        assert isinstance(testPeer._context, zmq.Context)
        assert testPeer._local_identity == "local_identity"
        assert testPeer._peer_identity == "peer_identity"
        assert not testPeer._is_started

    def test_setup(self):
        testPeer = peer.Peer(zmq.Context.instance(), "local_identity",
                             "peer_identity")
        testPeer._setup()
        assert isinstance(testPeer._zmq_dealer, zmq.sugar.socket.Socket)
        assert (testPeer._zmq_dealer.getsockopt(zmq.IDENTITY) ==
                six.b("local_identity"))
        assert testPeer._zmq_dealer.getsockopt(zmq.LINGER) == 0

    def test_start(self):
        mock_zmq_context = mock.Mock(spec=zmq.Context)
        testPeer = peer.Peer(mock_zmq_context, "local_identity",
                             "peer_identity")
        testPeer.start()
        mock_connect = testPeer._zmq_dealer.connect
        mock_connect.assert_called_once_with("tcp://peer_identity")
        assert testPeer._is_started

    def test_stop(self):
        mock_zmq_context = mock.Mock(spec=zmq.Context)
        testPeer = peer.Peer(mock_zmq_context, "local_identity",
                             "peer_identity")
        testPeer.start()
        testPeer.stop()
        testPeer._zmq_dealer.close.assert_called_once_with()
        assert not testPeer._is_started

    def test_send_message(self):
        mock_zmq_context = mock.Mock(spec=zmq.Context)
        testPeer = peer.Peer(mock_zmq_context, "local_identity",
                             "peer_identity")
        testPeer.start()
        testPeer.send_message(b"test_message")
        testPeer._zmq_dealer.send.assert_called_once_with(b"test_message",
                                                          flags=zmq.NOBLOCK)

    def test_send_message_zmq_error(self):
        mock_zmq_context = mock.Mock(spec=zmq.Context)
        testPeer = peer.Peer(mock_zmq_context, "local_identity",
                             "peer_identity")
        testPeer.start()
        testPeer._zmq_dealer.send.side_effect = zmq.ZMQError
        testPeer.send_message(b"test_message")
