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

from . import commands
from . import datatree
from . import log

import argparse
import logging
import os
import sys
import threading
import uuid

import six
import zmq
from zmq.eventloop import ioloop

LOG = logging.getLogger(__name__)


class Client(object):
    """Chillaxd client.

    This client use a separate thread in order to receive responses
    asynchronously from the server.
    """

    def __init__(self, chillaxd_endpoint):
        """:param chillaxd_endpoint: address of the server on which the
        client will connect to, in the form of "address ip:port".
        :type chillaxd_endpoint: six.string_types
        """
        super(Client, self).__init__()
        self._chillaxd_endpoint = chillaxd_endpoint

        # Local event loop used to asynchronously send commands and receive the
        # responses asynchronously as well.
        self._event_loop = None

        # Local ZeroMQ PAIR socket to talk with the local event loop.
        self._zmq_pair = None

        # Data structure used to store the async results. The async result is
        # created on the client side and the response is set by the event loop.
        # It associates a command id to an AsyncResult object.
        # TODO(yassine): use a thread safe dict
        self._sync_responses = {}

        # A client identifier.
        self._client_id = six.b(str(uuid.uuid4()))

        # Indicate if the client is started.
        self._is_started = False

    def _setup(self):
        """Setup all the attributes.

        Start the local event loop and connect the local PAIR socket to it.
        """
        context = zmq.Context()
        self._zmq_pair = context.socket(zmq.PAIR)
        self._zmq_pair.setsockopt(zmq.LINGER, 0)
        self._zmq_pair.connect("inproc://%s" % self._client_id)
        self._event_loop = ClientEventLoop(context, self._client_id,
                                           self._chillaxd_endpoint,
                                           self._sync_responses)
        self._event_loop.setDaemon(True)
        self._event_loop.start()

    def start(self):
        """Start the connection to the server."""
        if not self._is_started:
            self._setup()
            self._is_started = True

    def stop(self):
        """Close the connection."""
        if self._is_started:
            # todo(yassine): send STOP command and join() the thread
            self._zmq_pair.close()
            self._is_started = False

    def _create_async_result(self, cmd_id):
        """Create an async result and save it on self._sync_responses.

        :param cmd_id: The id of the command.
        :type cmd_id: six.string_types
        :return: An async result.
        :rtype: AsyncResult
        """
        async_result = AsyncResult(cmd_id)
        self._sync_responses[cmd_id] = async_result
        return async_result

    def create_node(self, path, data):
        """Create a node referenced by a path and associate data.

        :param path: The path of the node to create.
        :type path: six.string_types
        :param data: The associated data.
        :type data: bytearray
        :return: An async result, the get() method will return the status.
        :rtype: AsyncResult
        """
        cmd_id, create_node_cmd = commands.build_create_node(path, data)
        async_result = self._create_async_result(cmd_id)
        self._zmq_pair.send(create_node_cmd)
        return async_result

    def delete_node(self, path):
        """Delete a node referenced by a path.

        :param path: The path of the node to delete.
        :type path: six.string_types
        :return: An async result, the get() method will return the status.
        :rtype: AsyncResult
        """
        cmd_id, delete_node_cmd = commands.build_delete_node(path)
        async_result = self._create_async_result(cmd_id)
        self._zmq_pair.send(delete_node_cmd)
        return async_result

    def get_children(self, path):
        """Get the children of a node referenced by a path.

        :param path: The path of the node.
        :type path: six.string_types
        :return: An async result, the get() method will return a list of each
        child name.
        :rtype: AsyncResult
        """
        cmd_id, get_children_cmd = commands.build_get_children(path)
        async_result = self._create_async_result(cmd_id)
        self._zmq_pair.send(get_children_cmd)
        return async_result

    def get_data(self, path):
        """Get the data of a node referenced by a path.

        :param path: The path of the node.
        :type path: six.string_types
        :return: An async result, the get() method will return the associated
        data.
        :rtype: AsyncResult
        """
        cmd_id, get_data_cmd = commands.build_get_data(path)
        async_result = self._create_async_result(cmd_id)
        self._zmq_pair.send(get_data_cmd)
        return async_result

    def set_data(self, path, data):
        """Set the data of a node referenced by a path.

        :param path: The path of the node.
        :type path: six.string_types
        :param data: The data to set.
        :type data: bytearray
        :return: An async result, the get() method will return the status.
        :rtype: AsyncResult
        """
        cmd_id, set_data_cmd = commands.build_set_data(path, data)
        async_result = self._create_async_result(cmd_id)
        self._zmq_pair.send(set_data_cmd)
        return async_result


class ChillaxTimeoutException(Exception):
    """Raised when a command time out."""


class AsyncResult(object):
    """An async result.

    Every call returns an AsyncResult object which permit to send a request
    and get the result later.
    """
    def __init__(self, command_id):
        """:param command_id: The id of the command
        :type command_id: six.string_types
        """
        self._command_id = command_id

        # Once the result is received, the event loop will set the _event flag
        # in order to notify the listeners.
        self._event = threading.Event()

        # The result of the command.
        self._result = None

    def _set_result(self, result):
        """Set the result.

        :param result: The result of the command.
        :type result: object
        """
        self._result = result
        self._event.set()

    def get(self, timeout=None):
        """Get the result.

        If timeout is None then the method will block indefinitely. If the
        method time out it raises ChillaxTimeoutException.

        :param timeout: The timeout in seconds.
        :type timeout: int
        :return: The result according to the command.
        :rtype: object
        """
        if not self._event.wait(timeout=timeout):
            raise ChillaxTimeoutException()
        return commands.decode_command(self._result)

    def get_command_id(self):
        """Get the command id.

        :return: The associated command id.
        :rtype: bytearray
        """
        return self._command_id


class ClientEventLoop(threading.Thread):
    """The event loop in charge of managing the connection with Chillaxd and
    receiving results.

    The event loop is running in a separate thread, it binds to a local
    ZeroMQ socket, using the inproc transport, so that the client API can
    interact with it.
    """

    def __init__(self, zmq_context, client_id, chillaxd_endpoint,
                 sync_response):
        """:param zmq_context: A ZeroMQ context.
        :type zmq_context: zmq.Context
        :param client_id: The client ID.
        :type client_id: six.string_types
        :param chillaxd_endpoint: The chillaxd address in the form
        address:port'
        :type chillaxd_endpoint: six.string_types
        :param sync_response: The structure on which the synchronisation is
        done between the client API thread and the event loop.
        :type: dict
        """
        super(ClientEventLoop, self).__init__()
        self._zmq_context = zmq_context

        # The identifier of the client.
        self._client_id = client_id

        # Chillaxd server address:port
        self._chillaxd_endpoint = chillaxd_endpoint

        # Dict used for the synchronization.
        self._sync_response = sync_response

        # The socket on which to bind in order to receive local commnds.
        self._socket_for_commands = None

        # The ZeroMQ event loop.
        self._zmq_ioloop = None

        # The socket used to send commands to the server.
        self._zmq_dealer = None

    def _setup(self):
        """Setup all the attributes.

        Bind the PAIR socket, connect to chillaxd server.
        """
        # A ZeroMQ PAIR socket is used so that the connection between the
        # client and the event loop is exclusive.
        self._socket_for_commands = self._zmq_context.socket(zmq.PAIR)
        self._socket_for_commands.bind("inproc://%s" % self._client_id)
        self._zmq_dealer = self._zmq_context.socket(zmq.DEALER)
        self._zmq_dealer.connect("tcp://%s" % self._chillaxd_endpoint)
        LOG.debug("connecting to chillaxd server '%s'" %
                  self._chillaxd_endpoint)
        self._zmq_dealer.setsockopt(zmq.IDENTITY, self._client_id)
        self._zmq_dealer.setsockopt(zmq.LINGER, 0)
        self._zmq_ioloop = ioloop.ZMQIOLoop().instance()
        self._zmq_ioloop.add_handler(self._socket_for_commands,
                                     self._process_command_message,
                                     zmq.POLLIN)
        self._zmq_ioloop.add_handler(self._zmq_dealer,
                                     self._process_response_message,
                                     zmq.POLLIN)

    # todo(yassine): add commands protocol between client and local event loop,
    # messages: SEND, STOP
    def _process_command_message(self, socket_for_commands, event):
        """Process a command message.

        :param socket_for_commands: The zmq.PAIR socket.
        :type socket_for_commands: zmq.sugar.socket.Socket
        :param event: The corresponding event, it should only be zmq.POLLIN.
        :type event: int
        """
        command = socket_for_commands.recv()
        # Simply send the command.
        self._zmq_dealer.send(command)

    def _process_response_message(self, socket_dealer, event):
        """Process a response message.

        Decode the message and set the result.

        :param socket_dealer: The zmq.DEALER socket.
        :type socket_dealer: zmq.sugar.socket.Socket
        :param event: The corresponding event, it should only be zmq.POLLIN.
        :type event: int
        """
        response_message = socket_dealer.recv()
        _, response_id, response = commands.decode_command(response_message)
        self._sync_response[response_id]._set_result(response_message)
        del self._sync_response[response_id]

    def run(self):
        self._setup()
        self._zmq_ioloop.start()


def _init_conf():
    parser = argparse.ArgumentParser(description='Chillaxd client.')
    command_subparser = parser.add_subparsers(help='commands',
                                              dest='command')
    # create node
    create_node_parser = command_subparser.add_parser(
        'create_node', help='create a node')
    create_node_parser.add_argument('path', action='store',
                                    help='the node path')
    create_node_parser.add_argument('data', action='store',
                                    help='the associated data')

    # delete node
    delete_node_parser = command_subparser.add_parser(
        'delete_node', help='delete a node')
    delete_node_parser.add_argument('path', action='store',
                                    help='node path')

    # set data node
    set_data_node_parser = command_subparser.add_parser(
        'set_data', help='set data node')
    set_data_node_parser.add_argument('path', action='store',
                                      help='the node path')
    set_data_node_parser.add_argument('data', action='store',
                                      help='the data to set')

    # get data node
    get_data_node_parser = command_subparser.add_parser(
        'get_data', help='get data of a node')
    get_data_node_parser.add_argument('path', action='store',
                                      help='node path')

    # get children
    get_children_parser = command_subparser.add_parser(
        'get_children', help='get children of a node')
    get_children_parser.add_argument('path', action='store',
                                     help='node path')
    return parser.parse_args()


def main():

    conf = _init_conf()
    log.setup_logging()

    chillaxd_client = Client(os.environ.get("CHILLAXD_SERVER",
                                            "127.0.0.1:27001"))
    chillaxd_client.start()

    if conf.command == "create_node":
        async_result = chillaxd_client.create_node(conf.path, conf.data)
        _, response_id, response = async_result.get()

        if response[0] == datatree.NodeExistsException.errno:
            raise datatree.NodeExistsException()
        elif response[0] == datatree.NoNodeException.errno:
            raise datatree.NoNodeException()
        else:
            print("ACK command '%s'" % response_id)
    elif conf.command == "delete_node":
        async_result = chillaxd_client.delete_node(conf.path)
        _, response_id, response = async_result.get()

        if response[0] == datatree.NotEmptyException.errno:
            raise datatree.NotEmptyException()
        elif response[0] == datatree.NoNodeException.errno:
            raise datatree.NoNodeException()
        else:
            print("ACK command '%s'" % response_id)
    elif conf.command == "get_data":
        async_result = chillaxd_client.get_data(conf.path)
        _, response_id, response = async_result.get()

        if response[0] == datatree.NoNodeException.errno:
            raise datatree.NoNodeException()
        else:
            print(response[1])
    elif conf.command == "set_data":
        async_result = chillaxd_client.set_data(conf.path, conf.data)
        _, response_id, response = async_result.get()

        if response[0] == datatree.NodeExistsException.errno:
            raise datatree.NodeExistsException()
        elif response[0] == datatree.NoNodeException.errno:
            raise datatree.NoNodeException()
        else:
            print("ACK command '%s' " % response_id)
    elif conf.command == "get_children":
        async_result = chillaxd_client.get_children(conf.path)
        response_type, response_id, response = async_result.get()

        if response[0] == datatree.NoNodeException.errno:
            raise datatree.NoNodeException()
        else:
            l_children = list(response[1])
            l_children.sort()
            print(l_children)
    elif sys.argv[1] == "create_test":
        responses = []
        for i in xrange(15000):
            async_result = chillaxd_client.create_node("/test" + str(i),
                                                       "/test" + str(i))
            responses.append(async_result)

        for i in xrange(15000):
            _, response_id, response = responses[i].get()

            if response[0] == datatree.NodeExistsException.errno:
                raise datatree.NodeExistsException()
            elif response[0] == datatree.NoNodeException.errno:
                raise datatree.NoNodeException()
            else:
                print("ACK command '%s'" % response_id)
    elif sys.argv[1] == "get_test":
        responses = []
        for i in xrange(15000):
            async_result = chillaxd_client.get_data("/test" + str(i))
            responses.append(async_result)

        for i in xrange(15000):
            _, response_id, response = responses[i].get()

            if response[0] == datatree.NoNodeException.errno:
                raise datatree.NoNodeException()
            else:
                print(response[1])

    chillaxd_client.stop()

if __name__ == '__main__':
    main()
