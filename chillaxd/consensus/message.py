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

# The different message types according to RAFT.
APPEND_ENTRY_REQUEST = 1
APPEND_ENTRY_RESPONSE = 2
REQUEST_VOTE = 3
REQUEST_VOTE_RESPONSE = 4


def build_append_entry_request(term, prev_log_index, prev_log_term,
                               commit_index, entries):
    append_entry_request = (APPEND_ENTRY_REQUEST, term, prev_log_index,
                            prev_log_term, commit_index, entries)
    return msgpack.packb(append_entry_request)


def build_append_entry_response(term, success, last_log_index):
    append_entry_response = (APPEND_ENTRY_RESPONSE, term, success,
                             last_log_index)
    return msgpack.packb(append_entry_response)


def build_request_vote(term, last_log_index, last_log_term):
    request_vote = (REQUEST_VOTE, term, last_log_index, last_log_term)
    return msgpack.packb(request_vote)


def build_request_vote_response(term, vote_granted):
    request_vote_response = (REQUEST_VOTE_RESPONSE, term, vote_granted)
    return msgpack.packb(request_vote_response)


def decode_message(message):
    """Decode a message.

    :param message: The message.
    :type message: six.binary_type
    :return: The decoded message in the form of
    (MESSAGE TYPE, data1, ..., data2)
    """
    decoded_message = msgpack.unpackb(message, use_list=False)
    return decoded_message[0], decoded_message[1:]
