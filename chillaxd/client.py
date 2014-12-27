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

import commands

import logging
import sys
import uuid

import colorlog
import six
import zmq

LOG = logging.getLogger(__name__)


class ChillaxdClient(object):

    def __init__(self, chillaxd_server_endpoint):
        super(ChillaxdClient, self).__init__()
        self._chillaxd_server_endpoint = chillaxd_server_endpoint
        self._zmq_dealer = None
        self._is_started = False

    def _setup(self):
        """Setup the attributes.

        Set the identity into the zmq DEALER socket.
        """
        context = zmq.Context()
        self._zmq_dealer = context.socket(zmq.DEALER)
        self._zmq_dealer.setsockopt(zmq.IDENTITY, six.b(str(uuid.uuid4())))
        self._zmq_dealer.setsockopt(zmq.LINGER, 0)

    def start(self):
        """Start the connection to Chillaxd server."""
        if not self._is_started:
            self._setup()
            self._zmq_dealer.connect("tcp://%s" %
                                     self._chillaxd_server_endpoint)
            self._is_started = True
            LOG.debug("connecting to chillaxd server '%s'" %
                      self._chillaxd_server_endpoint)

    def stop(self):
        """Close the connection."""
        if self._is_started:
            self._zmq_dealer.close()
            self._is_started = False

    def create_node(self, path, data):
        create_node_cmd = commands.build_create_node(path, data)
        self._zmq_dealer.send(create_node_cmd)

    def delete_node(self, path):
        delete_node_cmd = commands.build_create_node(path)
        self._zmq_dealer.send(delete_node_cmd)

    def get_children(self, path):
        get_children_cmd = commands.build_get_children_request(path)
        self._zmq_dealer.send(get_children_cmd)
        return self._zmq_dealer.recv_multipart()[0]

    def get_data(self, path):
        get_data_cmd = commands.build_get_data(path)
        self._zmq_dealer.send(get_data_cmd)
        decoded_response = commands.decode_command(
            self._zmq_dealer.recv_multipart()[0])
        return decoded_response[1]

    def set_data(self, path, data):
        set_data_cmd = commands.build_set_data(path, data)
        self._zmq_dealer.send(set_data_cmd)


def _setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s :: %(levelname)s :: %(message)s",
        datefmt=None,
        reset=True,
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red'
        }
    )
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)


def main():
    _setup_logging()
    chillaxd_client = ChillaxdClient(sys.argv[1])
    chillaxd_client.start()

    if sys.argv[2] == "create_node":
        chillaxd_client.create_node(sys.argv[3], sys.argv[4])
    if sys.argv[2] == "delete_node":
        chillaxd_client.delete_node(sys.argv[3])
    elif sys.argv[2] == "get_data":
        print chillaxd_client.get_data(sys.argv[3])
    elif sys.argv[2] == "set_data":
        chillaxd_client.set_data(sys.argv[3], sys.argv[4])
    elif sys.argv[2] == "get_children":
        command_response = chillaxd_client.get_children(sys.argv[3])
        children = commands.decode_command(command_response)
        l_children = list(children[1])
        l_children.sort()
        print l_children

    chillaxd_client.stop()

if __name__ == '__main__':
    main()
