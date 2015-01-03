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
import six


class RaftLog(object):

    def __init__(self):
        self._log = {}
        self._prev_last_log_index = 0
        self._prev_last_log_term = 0
        self._last_index = 0
        self._last_term = 0

        # nil entry for index 0
        self._log[0] = _build_log_entry(0, 0, None)

    def append_entry(self, term, command):
        self._prev_last_log_index = self._last_index
        self._prev_last_log_term = self._last_term
        self._last_index += 1
        self._last_term = term
        entry = _build_log_entry(self._last_index,
                                 self._last_term, command)
        self._log[self._last_index] = entry

    def add_entries_at_start_index(self, start_index, entries):
        if not entries:
            return

        index = start_index
        for entry in entries:
            self._log[index] = entry
            index += 1
        last_entry = self.entry_at_index(len(self._log) - 1, decode=True)
        self._last_index = last_entry[0]
        self._last_term = last_entry[1]

        prev_last_entry = self.entry_at_index(index - 1, decode=True)
        self._prev_last_log_index = prev_last_entry[0]
        self._prev_last_log_term = prev_last_entry[1]

    def entries_from_index(self, start_index):
        ret = []
        for index in six.moves.range(start_index, self._last_index + 1):
            ret.append(self._log[index])
        return ret

    def entry_at_index(self, index, decode=False):
        try:
            if decode:
                return decode_log_entry(self._log[index])
            else:
                return self._log[index]
        except KeyError:
            return self._log[0]

    def last_index(self):
        return self._last_index

    def last_term(self):
        return self._last_term

    def prev_index_and_term_of_entry(self, index):
        if index <= 0:
            prev_entry = self.entry_at_index(0, decode=True)
            return prev_entry[0], prev_entry[1]
        else:
            prev_entry = self.entry_at_index(index - 1, decode=True)
            return prev_entry[0], prev_entry[1]

    def index_and_term_of_last_entry(self):
        return self._last_index, self._last_term


def _build_log_entry(index, term, command):
    log_entry = (index, term, command)
    return msgpack.packb(log_entry)


def decode_log_entry(log_entry):
    return msgpack.unpackb(log_entry, use_list=False)
