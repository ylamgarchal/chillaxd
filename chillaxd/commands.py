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


def build_create_node(path, data):
    create_node = (CREATE_NODE, path, data)
    return msgpack.packb(create_node)


def build_delete_node(path):
    delete_node = (DELETE_NODE, path)
    return msgpack.packb(delete_node)


def build_get_children_request(path):
    get_children_request = (GET_CHILDREN, path)
    return msgpack.packb(get_children_request)


def build_get_children_response(children):
    get_children_response = (GET_CHILDREN_RESPONSE, children)
    return msgpack.packb(get_children_response)


def build_get_data(path):
    get_data_request = (GET_DATA, path)
    return msgpack.packb(get_data_request)


def build_get_data_response(data):
    get_data_response = (GET_DATA_RESPONSE, data)
    return msgpack.packb(get_data_response)


def build_set_data(path, data):
    set_data = (SET_DATA, path, data)
    return msgpack.packb(set_data)


def build_no_operation():
    no_operation = (NO_OPERATION, )
    return msgpack.packb(no_operation)


def decode_command(command):
    return msgpack.unpackb(command, use_list=False)
