from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

from collections import defaultdict

from twitter.common.collections import OrderedSet

from twitter.pants.base.address import Address


import logging
logger = logger.getLogger(__name__)


class BuildGraph(object):
  '''
  A directed acyclic graph of Targets and Dependencies representing the dependencies of a project.
  Not necessarily connected.  Always serializable.
  '''

  def __init__(self):
    self._targets_by_address = {}

