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


class RaftLog(object):

    def __init__(self):
        self._log = {}
        self._current_log_index = -1
        self._current_log_term = -1
        self._prev_log_index = -1
        self._prev_log_term = -1

        # nil entry for index -1
        self._log[-1] = _build_log_entry(-1, -1, None)

    def append_entry(self, term, command):
        self._prev_log_index = self._current_log_index
        self._prev_log_term = self._current_log_term
        self._current_log_index += 1
        self._current_log_term = term
        entry = _build_log_entry(self._current_log_index,
                                 self._current_log_term, command)
        self._log[self._current_log_index] = entry

    def add_entries_at_start_index(self, start_index, entries):
        index = start_index
        for entry in entries:
            self._log[index] = entry
            index += 1

    def entry_at_index(self, index, decode=False):
        if decode:
            return decode_log_entry(self._log[index])
        else:
            return self._log[index]

    def last_entry(self):
        return self._log[self._current_log_index]

    def last_index(self):
        return self._current_log_index

    def index_and_term_from_prev_last_entry(self):
        return self._prev_log_index, self._prev_log_term

    def index_and_term_from_last_entry(self):
        return self._current_log_index, self._current_log_term

    def remove_entries_from_index(self, index):
        for i in self._log:
            if i >= index:
                del self._log[index]


def _build_log_entry(index, term, command):
    log_entry = (index, term, command)
    return msgpack.packb(log_entry)


def decode_log_entry(log_entry):
    return msgpack.unpackb(log_entry, use_list=False)
