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

from __future__ import absolute_import
from . import datanode

import six

# the root of chillaxd tree
_CHILLAXD_ROOT = "/"


class DataTree(object):
    """This class represents the current tree state.

    The tree maintains a dictionary that maps from absolute paths to
    DataNodes.
    """

    def __init__(self):
        super(DataTree, self).__init__()
        self._nodes = {}
        # DataNode root of the tree
        self._root = datanode.DataNode(data=six.binary_type())
        # Aliases for the root node in the tree
        self._nodes.setdefault("", self._root)
        self._nodes.setdefault(_CHILLAXD_ROOT, self._root)

    def create_node(self, path, data):
        """Add a new node to the tree.

        :param path: the path of the new node
        :type path: six.string_types
        :param data: the data of that node
        :type data: six.binary_type
        :return:
        """

        parent_path, child_name = self._get_parent_path_and_child_name(path)
        parent = self._nodes.get(parent_path)

        if not parent:
            raise NoNodeException()

        children = parent.get_children()
        if child_name in children:
            raise NodeExistsException()

        child = datanode.DataNode(data)
        parent.add_child(child_name)
        self._nodes[path] = child

    def delete_node(self, path):
        """Delete a node from the tree.

        :param path: the path of the node
        :type path: six.string_types
        :return:
        """

        parent_path, child_name = self._get_parent_path_and_child_name(path)
        node = self._nodes.get(path)
        if not node:
            raise NoNodeException()

        if len(node.get_children()) > 0:
            raise NotEmptyException()

        del self._nodes[path]

        parent = self._nodes.get(parent_path)
        parent.remove_child(child_name)

    def set_data(self, path, data):
        """Set data to a node.

        :param path: the path of the node
        :type path: six.stirng_types
        :param data: the data to be set
        :type data: six.binary_type
        :return:
        """

        node = self._nodes.get(path)
        if not node:
            raise NoNodeException()

        node.set_data(data)

    def get_data(self, path):
        """Get the data of a node.

        :param path: the path of the node
        :type path: six.string_types
        :return: the data of the node
        :type: six.binary_type
        """

        node = self._nodes.get(path)
        if not node:
            raise NoNodeException()
        else:
            return node.get_data()

    def get_children(self, path):
        """Get the children of a node.

        :param path: the of path of the node
        :type path: six.string_types
        :return: set of children's name
        :type: set
        """

        node = self._nodes.get(path)
        if not node:
            raise NoNodeException()
        else:
            return node.get_children()

    @staticmethod
    def _get_parent_path_and_child_name(path):
        """Split the path into the parent path and child name.

        For instance, "/a/b/c" -> ("/a/b", "c")
        :param path: the path to split
        :type path: six.string_types
        :return: a tuple in the form (parent path, child name)
        :type: tuple
        """

        last_slash = path.rfind('/')
        parent_path = path[:last_slash]
        child_name = path[last_slash + 1:]
        return parent_path, child_name


class NoNodeException(Exception):
    """Raised when a node is not found."""


class NodeExistsException(Exception):
    """Raised when a node already exists."""


class NotEmptyException(Exception):
    """Raised when trying to delete a node with children."""
