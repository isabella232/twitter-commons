from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

from collections import defaultdict

from twitter.common.collections import OrderedSet

from twitter.pants.base.address import Address


import logging
logger = logging.getLogger(__name__)


class BuildGraph(object):
  '''
  A directed acyclic graph of Targets and Dependencies representing the dependencies of a project.
  Not necessarily connected.  Always serializable.
  '''

  def __init__(self):
    self._target_by_address = {}
    self._target_dependencies_by_address = defaultdict(set)
    self._target_dependents_by_address = defaultdict(set)

  def contains_address(self, address):
    return address in self._target_by_address

  def dependencies_of(self, address):
    assert address in self._target_by_address, (
      'Cannot retrieve dependencies of {address} because it is not in the BuildGraph.'
      .format(address=address)
    )
    return self._target_dependencies_by_address[address]

  def dependents_of(self, address):
    assert address in self._target_by_address, (
      'Cannot retrieve dependents of {address} because it is not in the BuildGraph.'
      .format(address=address)
    )
    return self._target_dependents_by_address[address]

  def inject_target(self, target, dependencies=None):
    dependencies = dependencies or frozenset()
    address = target.address

    assert address not in self._target_by_address, (
      'A Target {existing_target} already exists in the BuildGraph at address {address}.'
      ' Failed to insert {target}.'
      .format(existing_target=self._target_by_address[address],
              address=address,
              target=target)
    )

    self._target_by_address[address] = target

    for dependency_address in dependencies:
      self.inject_dependency(dependent=address, dependency=dependency_address)

  def inject_dependency(self, dependent, dependency):
    assert dependent in self._target_by_address, (
      'Cannot inject dependency from {dependent} on {dependency} because the dependent is not'
      ' in the BuildGraph.'
      .format(dependent=dependent, dependency=dependency)
    )
    assert dependency in self._target_by_address, (
      'Cannot inject dependency from {dependent} on {dependency} because the dependency is not'
      ' in the BuildGraph.  This probably indicates a dependency cycle.'
      .format(dependent=dependent, dependency=dependency)
    )
    if dependency in self.dependencies_of(dependent):
      logger.warn('{dependent} already depends on {dependency}'
                  .format(dependent=dependent, dependency=dependency))
    else:
      self._target_dependencies_by_address[dependent].add(dependency)
      self._target_dependents_by_address[dependency].add(dependent)
