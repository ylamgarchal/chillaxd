# -*- coding: utf-8 -*-
# Author: Yassine Lamgarchal <lamgarchal.yassine@gmail.com>>
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
import six

from chillaxd import datanode


class TestDataNode(object):

    def setup_method(self, method):
        self.test_dn = datanode.DataNode(six.binary_type(b"test_dn_data"))

    def test_create_data_node(self):
        test_dn = datanode.DataNode(six.binary_type(b"test_dn_data"))
        assert set() == test_dn.get_children()

    def test_get_and_add_children(self):
        self.test_dn.add_child("test_child_1")
        assert set({"test_child_1"}) == self.test_dn.get_children()

    def test_get_and_remove_children(self):
        self.test_dn.add_child("test_child_1")
        self.test_dn.add_child("test_child_2")
        assert {"test_child_1", "test_child_2"} == self.test_dn.get_children()
        self.test_dn.remove_child("test_child_1")
        assert set({"test_child_2"}) == self.test_dn.get_children()

    def test_get_and_set_data(self):
        assert six.binary_type(b"test_dn_data") == self.test_dn.get_data()
        self.test_dn.set_data(six.binary_type(b"new_test_dn_data"))
        assert six.binary_type(b"new_test_dn_data") == self.test_dn.get_data()
