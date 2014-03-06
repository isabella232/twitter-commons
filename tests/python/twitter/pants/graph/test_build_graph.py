# ==================================================================================================
# Copyright 2013 Twitter, Inc.
# --------------------------------------------------------------------------------------------------
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this work except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file, or at:
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==================================================================================================

import os
import pytest
import unittest

from contextlib import contextmanager

from twitter.common.contextutil import pushd, temporary_dir
from twitter.common.dirutil import touch

from twitter.pants.base.build_file import BuildFile
from twitter.pants.base.address import Address, SyntheticAddress, BuildFileAddress
from twitter.pants.graph.build_graph import BuildGraph
from twitter.pants.base.build_environment import set_buildroot


class BuildGraphTest(unittest.TestCase):

  def test_synthetic_forms(self):
    self.assertAddress('a/b', 'target', SyntheticAddress('a/b:target'))
    self.assertAddress('a/b', 'b', SyntheticAddress('a/b'))
    self.assertAddress('a/b', 'target', SyntheticAddress(':target', 'a/b'))
    self.assertAddress('', 'target', SyntheticAddress(':target'))

