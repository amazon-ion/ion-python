# Copyright 2016 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at:
#
#    http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS
# OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the
# License.

"""Buffer for binary Ion writers."""

# Python 2/3 compatibility
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class _Node:
    def __init__(self, value=None):
        self.value = value
        self.parent = None
        self.children = None

    def add_child(self, node):
        if self.children is None:
            self.children = []
        node.parent = self
        self.children.append(node)

    def add_leaf(self, node):
        if not self.children:
            self.add_child(node)
            return
        leftmost_child = self.children[0]
        while leftmost_child.children:
            leftmost_child = leftmost_child.children[0]
        leftmost_child.add_child(node)


class BufferTree:
    """A tree of buffers that can be depth-first traversed to produce a
    correctly-ordered Ion stream.

    Containers are treated as subtrees. Calling ``start_container`` creates
    an empty node which represents the start of a new subtree, while calling
    ``end_container`` steps out of that subtree, allowing for additional
    nodes to be added as siblings of that subtree's start node.
    It is important that there is exactly one call to start_container per call
    to ``end_container``. Nodes representing the start of a container subtree
    are kept in a stack; upon calling ``end_container``, the subtree represented
    by the node that was created by the most recent call to start_container will
    be stepped out. Other than that, no container semantics are asserted by this class.

    Scalar values are treated as child nodes of the current container subtree
    (which may be at the top-level if the current container subtree's root node
    is the root node of the BufferTree). Calling add_scalar_value will add a new
    node as a child of the current container subtree. A container subtree's
    children are ordered.

    If None values are passed in to end_container or add_scalar_value,
    nodes will be added to the tree, in the same way as described above, but no value
    will be yielded for that node upon drain.
    """
    def __init__(self):
        self.__root = None
        self.__container_lengths = None  # Stack of pending container lengths.
        self.__container_nodes = None  # Stack of pending container nodes.
        self.__container_node = None  # Node representing the currently active container.
        self.current_container_length = None  # Length of the currently active container.
        self.__reset()

    def __reset(self):
        self.__root = _Node()
        self.__container_lengths = []
        self.__container_nodes = []
        self.__container_node = self.__root
        self.current_container_length = 0

    def __depth_traverse(self, node):
        if node.children:
            for child in node.children:
                for val in self.__depth_traverse(child):
                    yield val
        yield node.value

    def start_container(self):
        """Add a node to the tree that represents the start of a container.

        Until end_container is called, any nodes added through add_scalar_value
        or start_container will be children of this new node.
        """
        self.__container_lengths.append(self.current_container_length)
        self.current_container_length = 0
        new_container_node = _Node()
        self.__container_node.add_child(new_container_node)
        self.__container_nodes.append(self.__container_node)
        self.__container_node = new_container_node

    def end_container(self, header_buf):
        """Add a node containing the container's header to the current subtree.

        This node will be added as the leftmost leaf of the subtree that was
        started by the matching call to start_container.

        Args:
            header_buf (bytearray): bytearray containing the container header.
        """
        if not self.__container_nodes:
            raise ValueError("Attempted to end container with none active.")
        # Header needs to be the first node visited on this subtree.
        self.__container_node.add_leaf(_Node(header_buf))
        self.__container_node = self.__container_nodes.pop()
        parent_container_length = self.__container_lengths.pop()
        self.current_container_length = \
            parent_container_length + self.current_container_length + len(header_buf)

    def add_scalar_value(self, value_buf):
        """Add a node to the tree containing a scalar value.

        Args:
            value_buf (bytearray): bytearray containing the scalar value.
        """
        self.__container_node.add_child(_Node(value_buf))
        self.current_container_length += len(value_buf)

    def drain(self):
        """Walk the BufferTree and reset it when finished.

        Yields:
            any: The current node's value.
        """
        if self.__container_nodes:
            raise ValueError("Attempted to drain without ending all containers.")
        for buf in self.__depth_traverse(self.__root):
            if buf is not None:
                yield buf
        self.__reset()

