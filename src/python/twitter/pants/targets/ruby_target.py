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

from twitter.common.collections import OrderedSet
from twitter.pants.targets.with_dependencies import TargetWithDependencies
from twitter.pants.targets.with_sources import TargetWithSources


class RubyTarget(TargetWithDependencies, TargetWithSources):
  """Base class for all Ruby targets."""

  def __init__(self,
               name,
               sources,
               dependencies=None,
               provides=None):
    TargetWithSources.__init__(self, name, sources=sources)
    TargetWithDependencies.__init__(self, name, dependencies=dependencies)
    self.provides = provides