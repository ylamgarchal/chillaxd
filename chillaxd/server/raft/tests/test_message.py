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

from chillaxd.server.raft import message

import six


def test_build_append_entry():
    test_term = 1
    payload = six.b("payload")
    ae_message = message.build_append_entry(test_term, payload)
    decoded_message = message.decode_message(ae_message)
    expected_message = (message.APPEND_ENTRY, test_term, payload)
    assert expected_message == decoded_message


def test_build_append_entry_response():
    test_term = 1
    payload = six.b("payload")
    aer_message = message.build_append_entry_response(test_term, payload)
    decoded_message = message.decode_message(aer_message)
    expected_message = (message.APPEND_ENTRY_RESPONSE, test_term, payload)
    assert expected_message == decoded_message


def test_build_request_vote():
    test_term = 1
    payload = six.b("payload")
    rv_message = message.build_request_vote(test_term, payload)
    decoded_message = message.decode_message(rv_message)
    expected_message = (message.REQUEST_VOTE, test_term, payload)
    assert expected_message == decoded_message


def test_build_request_vote_response():
    test_term = 1
    payload = six.b("payload")
    rvr_message = message.build_request_vote_response(test_term, payload)
    decoded_message = message.decode_message(rvr_message)
    expected_message = (message.REQUEST_VOTE_RESPONSE, test_term, payload)
    assert expected_message == decoded_message
