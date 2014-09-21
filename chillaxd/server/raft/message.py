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

# The different message types according to RAFT
# protocol, it uses MessagePack to serialize the messages.
APPEND_ENTRY = 1
APPEND_ENTRY_RESPONSE = 2
REQUEST_VOTE = 3
REQUEST_VOTE_RESPONSE = 4


def build_append_entry(term, payload):
    append_entry = (APPEND_ENTRY, term, payload)
    return msgpack.packb(append_entry)


def build_append_entry_response(term, payload):
    append_entry_response = (APPEND_ENTRY_RESPONSE, term, payload)
    return msgpack.packb(append_entry_response)


def build_request_vote(term, payload):
    request_vote = (REQUEST_VOTE, term, payload)
    return msgpack.packb(request_vote)


def build_request_vote_response(term, payload):
    request_vote_response = (REQUEST_VOTE_RESPONSE, term, payload)
    return msgpack.packb(request_vote_response)


def decode_message(message):
    return msgpack.unpackb(message, use_list=False)
