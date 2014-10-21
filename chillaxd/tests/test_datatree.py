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
import pytest
import six

from chillaxd import datatree


class TestDataTree(object):

    def setup_method(self, method):
        self.test_dt = datatree.DataTree()

    def test_create_node(self):
        self.test_dt.create_node("/a", six.binary_type(b"test_data_a"))
        assert set("a") == self.test_dt.get_children("/")
        assert six.binary_type(b"test_data_a") == self.test_dt.get_data("/a")

        self.test_dt.create_node("/a/b", six.binary_type(b"test_data_ab"))
        assert set("a") == self.test_dt.get_children("/")
        assert six.binary_type(b"test_data_a") == self.test_dt.get_data("/a")
        assert set("b") == self.test_dt.get_children("/a")
        assert (six.binary_type(b"test_data_ab") ==
                self.test_dt.get_data("/a/b"))
        self.test_dt.create_node("/a/c", six.binary_type(b"test_data_ac"))
        assert set("bc") == self.test_dt.get_children("/a")
        assert (six.binary_type(b"test_data_ac") ==
                self.test_dt.get_data("/a/c"))

    def test_create_node_with_no_parent(self):
        pytest.raises(datatree.NoNodeException, self.test_dt.create_node,
                      "/a/b", six.binary_type(b"test_data_a"))

    def test_create_node_with_existing_node(self):
        self.test_dt.create_node("/a", six.binary_type(b"test_data_a"))
        pytest.raises(datatree.NodeExistsException,
                      self.test_dt.create_node, "/a",
                      six.binary_type(b"test_data_a"))

    def test_delete_node(self):
        self.test_dt.create_node("/a", six.binary_type(b"test_data_a"))
        self.test_dt.create_node("/a/b", six.binary_type(b"test_data_ab"))
        self.test_dt.create_node("/a/c", six.binary_type(b"test_data_ac"))

        assert set("bc") == self.test_dt.get_children("/a")
        self.test_dt.delete_node("/a/c")
        assert set("b") == self.test_dt.get_children("/a")
        self.test_dt.delete_node("/a/b")
        assert set() == self.test_dt.get_children("/a")
        self.test_dt.delete_node("/a")
        assert set() == self.test_dt.get_children("/")

    def test_delete_node_with_nonexistent_node(self):
        pytest.raises(datatree.NoNodeException, self.test_dt.delete_node, "/a")
        pytest.raises(datatree.NoNodeException, self.test_dt.delete_node,
                      "/a/b")
        pytest.raises(datatree.NoNodeException, self.test_dt.delete_node,
                      "/b/c")

    def test_delete_node_with_children(self):
        self.test_dt.create_node("/a", six.binary_type(b"test_data_a"))
        self.test_dt.create_node("/a/b", six.binary_type(b"test_data_ab"))
        self.test_dt.create_node("/a/c", six.binary_type(b"test_data_ac"))

        pytest.raises(datatree.NotEmptyException, self.test_dt.delete_node,
                      "/a")

    def test_get_children(self):
        self.test_dt.create_node("/a", six.binary_type(b"test_data_a"))
        self.test_dt.create_node("/b", six.binary_type(b"test_data_b"))
        assert set("ab") == self.test_dt.get_children("/")
        pytest.raises(datatree.NoNodeException, self.test_dt.get_children,
                      "/c")
        pytest.raises(datatree.NoNodeException, self.test_dt.get_children,
                      "/a/c")

    def test_get_set_data(self):
        self.test_dt.create_node("/a", six.binary_type(b"test_data_a"))
        assert six.binary_type(b"test_data_a") == self.test_dt.get_data("/a")
        self.test_dt.set_data("/a", six.binary_type(b"test_data_new_a"))
        assert (six.binary_type(b"test_data_new_a") ==
                self.test_dt.get_data("/a"))

    def test_get_set_data_with_nonexistent_node(self):
        pytest.raises(datatree.NoNodeException, self.test_dt.set_data,
                      "/a", six.binary_type(b"test_data_new_a"))
        pytest.raises(datatree.NoNodeException, self.test_dt.get_data,
                      "/a")
        pytest.raises(datatree.NoNodeException, self.test_dt.set_data,
                      "/b", six.binary_type(b"test_data_new_a"))
        pytest.raises(datatree.NoNodeException, self.test_dt.get_data,
                      "/b")
        pytest.raises(datatree.NoNodeException, self.test_dt.set_data,
                      "/a/b", six.binary_type(b"test_data_new_a"))
        pytest.raises(datatree.NoNodeException, self.test_dt.get_data,
                      "/a/b")
