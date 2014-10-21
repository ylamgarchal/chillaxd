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

# The different commands that can be applied on the state machine.
CREATE_NODE = 1
DELETE_NODE = 2
GET_CHILDREN = 3
GET_DATA = 4
SET_DATA = 5


def build_create_node_message(path, data):
    create_node_message = (CREATE_NODE, path, data)
    return msgpack.packb(create_node_message)


def build_delete_node_message(path):
    delete_node_message = (DELETE_NODE, path)
    return msgpack.packb(delete_node_message)


def build_get_children_message(path):
    get_children_message = (GET_CHILDREN, path)
    return msgpack.packb(get_children_message)


def build_get_data_message(path):
    get_data_message = (GET_DATA, path)
    return msgpack.packb(get_data_message)


def build_set_data_message(path, data):
    set_data_message = (SET_DATA, path, data)
    return msgpack.packb(set_data_message)


def decode_command(command):
    return msgpack.unpackb(command, use_list=False)
