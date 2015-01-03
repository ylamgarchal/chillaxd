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


class DataNode(object):
    """This class represents a node in the tree.

    A data node contains a byte array as its data and a set of
    its children.
    """

    def __init__(self, data):
        """Initialize a data node with data.

        :param data: the data to be set
        :type data: six.binary_type
        :return:
        """

        super(DataNode, self).__init__()

        # the current data
        self._data = data
        # the set of children
        self._children = set()

    def add_child(self, child):
        """Add the child in the children set.

        :param child: the child to be added
        :type child: six.string_types
        :return:
        """

        self._children.add(child)

    def remove_child(self, child):
        """Remove the child from the children set.

        :param child: the child to be removed
        :type child: six.string_types
        :return:
        """

        self._children.remove(child)

    def get_children(self):
        """Get a copy of the children set.

        :return: the children set
        :type: set
        """

        return self._children.copy()

    def get_data(self):
        """Get the data.

        :return: the data
        :type: six.binary_type
        """

        return self._data

    def set_data(self, data):
        """Set a new data.

        :param data: the data to set
        :type data: six.binary_type
        :return:
        """

        self._data = data
