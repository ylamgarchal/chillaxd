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

import uuid

import msgpack

# The different commands that can be applied on the state machine and their
# corresponding responses.
CREATE_NODE = 10
CREATE_NODE_RESPONSE = 11
DELETE_NODE = 20
DELETE_NODE_RESPONSE = 21
GET_CHILDREN = 30
GET_CHILDREN_RESPONSE = 31
GET_DATA = 50
GET_DATA_RESPONSE = 51
SET_DATA = 60
SET_DATA_RESPONSE = 61
NO_OPERATION = 70

_READ_COMMANDS = [GET_CHILDREN, GET_DATA, NO_OPERATION]


def build_create_node(path, data):
    command_id = str(uuid.uuid4())
    create_node = (CREATE_NODE, command_id, path.encode("utf8"),
                   data.encode("utf8"))
    return command_id, msgpack.packb(create_node)


def build_create_node_response(command_id, success):
    create_node_response = (CREATE_NODE, command_id, success)
    return msgpack.packb(create_node_response)


def build_delete_node(path):
    command_id = str(uuid.uuid4())
    delete_node = (DELETE_NODE, command_id, path.encode("utf8"))
    return command_id, msgpack.packb(delete_node)


def build_delete_node_response(command_id, success):
    delete_node_response = (DELETE_NODE, command_id, success)
    return msgpack.packb(delete_node_response)


def build_get_children(path):
    command_id = str(uuid.uuid4())
    get_children_request = (GET_CHILDREN, command_id, path.encode("utf8"))
    return command_id, msgpack.packb(get_children_request)


def build_get_children_response(command_id, children):
    get_children_response = (GET_CHILDREN_RESPONSE, command_id, children)
    return msgpack.packb(get_children_response)


def build_get_data(path):
    command_id = str(uuid.uuid4())
    get_data_request = (GET_DATA, command_id, path.encode("utf8"))
    return command_id, msgpack.packb(get_data_request)


def build_get_data_response(command_id, data):
    get_data_response = (GET_DATA_RESPONSE, command_id, data.encode("utf8"))
    return msgpack.packb(get_data_response)


def build_set_data(path, data):
    command_id = str(uuid.uuid4())
    set_data = (SET_DATA, command_id, path.encode("utf8"), data.encode("utf8"))
    return command_id, msgpack.packb(set_data)


def build_set_data_response(command_id, success):
    set_data_response = (SET_DATA, command_id, success)
    return msgpack.packb(set_data_response)


def build_no_operation():
    command_id = str(uuid.uuid4())
    no_operation = (NO_OPERATION, command_id)
    return command_id, msgpack.packb(no_operation)


def is_read_command(command_type):
    return command_type in _READ_COMMANDS


def decode_command(command):
    decoded_command = msgpack.unpackb(command, use_list=False)
    return decoded_command[0], decoded_command[1], decoded_command[2:]
