# -*- coding: utf-8 -*-
# Copyright Yassine Lamgarchal <lamgarchal.yassine@gmail.com>
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

from chillaxd import datatree


class TestDataTree(object):

    def setup_method(self, method):
        self.test_dt = datatree.DataTree()

    def test_create_node(self):
        self.test_dt.create_node("/a".encode("utf8"),
                                 "test_data_a".encode("utf8"))
        assert ["a"] == self.test_dt.get_children("/".encode("utf8"))
        assert "test_data_a" == self.test_dt.get_data("/a".encode("utf8"))

        self.test_dt.create_node("/a/b".encode("utf8"),
                                 "test_data_ab".encode("utf8"))
        assert ["a"] == self.test_dt.get_children("/".encode("utf8"))
        assert "test_data_a" == self.test_dt.get_data("/a".encode("utf8"))
        assert ["b"] == self.test_dt.get_children("/a".encode("utf8"))
        assert "test_data_ab" == self.test_dt.get_data("/a/b".encode("utf8"))
        self.test_dt.create_node("/a/c".encode("utf8"),
                                 "test_data_ac".encode("utf8"))
        children = self.test_dt.get_children("/a".encode("utf8"))
        children.sort()
        assert ["b", "c"] == children
        assert "test_data_ac" == self.test_dt.get_data("/a/c".encode("utf8"))

    def test_create_node_with_no_parent(self):
        pytest.raises(datatree.NoNodeException, self.test_dt.create_node,
                      "/a/b".encode("utf8"), "test_data_a".encode("utf8"))

    def test_create_node_with_existing_node(self):
        self.test_dt.create_node("/a".encode("utf8"),
                                 "test_data_a".encode("utf8"))
        pytest.raises(datatree.NodeExistsException,
                      self.test_dt.create_node, "/a".encode("utf8"),
                      "test_data_a".encode("utf8"))

    def test_delete_node(self):
        self.test_dt.create_node("/a".encode("utf8"),
                                 "test_data_a".encode("utf8"))
        self.test_dt.create_node("/a/b".encode("utf8"),
                                 "test_data_ab".encode("utf8"))
        self.test_dt.create_node("/a/c".encode("utf8"),
                                 "test_data_ac".encode("utf8"))

        children = self.test_dt.get_children("/a".encode("utf8"))
        children.sort()
        assert ["b", "c"] == children
        self.test_dt.delete_node("/a/c".encode("utf8"))
        assert ["b"] == self.test_dt.get_children("/a".encode("utf8"))
        self.test_dt.delete_node("/a/b".encode("utf8"))
        assert [] == self.test_dt.get_children("/a".encode("utf8"))
        self.test_dt.delete_node("/a".encode("utf8"))
        assert [] == self.test_dt.get_children("/".encode("utf8"))

    def test_delete_node_with_nonexistent_node(self):
        pytest.raises(datatree.NoNodeException, self.test_dt.delete_node,
                      "/a".encode("utf8"))
        pytest.raises(datatree.NoNodeException, self.test_dt.delete_node,
                      "/a/b".encode("utf8"))
        pytest.raises(datatree.NoNodeException, self.test_dt.delete_node,
                      "/b/c".encode("utf8"))

    def test_delete_node_with_children(self):
        self.test_dt.create_node("/a".encode("utf8"),
                                 "test_data_a".encode("utf8"))
        self.test_dt.create_node("/a/b".encode("utf8"),
                                 "test_data_ab".encode("utf8"))
        self.test_dt.create_node("/a/c".encode("utf8"),
                                 "test_data_ac".encode("utf8"))

        pytest.raises(datatree.NotEmptyException, self.test_dt.delete_node,
                      "/a".encode("utf8"))

    def test_get_children(self):
        self.test_dt.create_node("/a".encode("utf8"),
                                 "test_data_a".encode("utf8"))
        self.test_dt.create_node("/b".encode("utf8"),
                                 "test_data_b".encode("utf8"))
        children = self.test_dt.get_children("/".encode("utf8"))
        children.sort()
        assert ["a", "b"] == children
        pytest.raises(datatree.NoNodeException, self.test_dt.get_children,
                      "/c".encode("utf8"))
        pytest.raises(datatree.NoNodeException, self.test_dt.get_children,
                      "/a/c".encode("utf8"))

    def test_get_set_data(self):
        self.test_dt.create_node("/a".encode("utf8"),
                                 "test_data_a".encode("utf8"))
        assert (str("test_data_a") ==
                self.test_dt.get_data("/a".encode("utf8")))
        self.test_dt.set_data("/a".encode("utf8"),
                              "test_data_new_a".encode("utf8"))
        assert (str("test_data_new_a") ==
                self.test_dt.get_data("/a".encode("utf8")))

    def test_get_set_data_with_nonexistent_node(self):
        pytest.raises(datatree.NoNodeException, self.test_dt.set_data,
                      "/a".encode("utf8"), "test_data_new_a".encode("utf8"))
        pytest.raises(datatree.NoNodeException, self.test_dt.get_data,
                      "/a".encode("utf8"))
        pytest.raises(datatree.NoNodeException, self.test_dt.set_data,
                      "/b".encode("utf8"), "test_data_new_a".encode("utf8"))
        pytest.raises(datatree.NoNodeException, self.test_dt.get_data,
                      "/b".encode("utf8"))
        pytest.raises(datatree.NoNodeException, self.test_dt.set_data,
                      "/a/b".encode("utf8"), "test_data_new_a".encode("utf8"))
        pytest.raises(datatree.NoNodeException, self.test_dt.get_data,
                      "/a/b".encode("utf8"))
