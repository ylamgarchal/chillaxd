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

from chillaxd.consensus import message


def test_build_append_entry_request():
    test_params = (1, 1, 2, 3, (4, 5))
    ae_message = message.build_append_entry_request(*test_params)
    decoded_message = message.decode_message(ae_message)
    assert (message.APPEND_ENTRY_REQUEST, test_params) == decoded_message


def test_build_append_entry_response():
    test_params = (0, True, 0)
    aer_message = message.build_append_entry_response(*test_params)
    decoded_message = message.decode_message(aer_message)
    assert (message.APPEND_ENTRY_RESPONSE, test_params) == decoded_message


def test_build_request_vote():
    test_params = (0, 1, 2)
    rv_message = message.build_request_vote(*test_params)
    decoded_message = message.decode_message(rv_message)
    assert (message.REQUEST_VOTE, test_params) == decoded_message


def test_build_request_vote_response():
    test_params = (0, False)
    rvr_message = message.build_request_vote_response(*test_params)
    decoded_message = message.decode_message(rvr_message)
    assert (message.REQUEST_VOTE_RESPONSE, test_params) == decoded_message
