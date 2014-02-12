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

  def __init__(self, build_root):
    self._build_root = build_root
    self._targets_by_address = {}
    self._addresses_by_buildfile = defaultdict(OrderedSet)
    self._added_buildfiles = set()

  def add_spec(self, spec):
    '''
    Add a spec (a string) to the build graph.  This method should be idempotent for equivalent specs
    '''

    address = Address.parse(self._build_root, spec)
    self.add_address(address)

  def add_address(self, address):
    '''
    Add an Address object to the build graph by parsing its BUILD file.  This method should be idempotent for equivalent addresses (and in fact for any set of addresses that live in the same BUILD file.)
    '''

    logger.debug('Adding Address %s to the build graph.' % address)

    buildfile = address.buildfile

    if address in self._targets_by_address:
      logger.debug('Address %s already added to the build graph.')
      assert(buildfile in self._added_buildfiles,
             'Address %s from spec <%s> has been added to the BuildGraph, but no buildfile was'
             ' found in _added_buildfiles' % (address, spec))
      return

    if buildfile in self._added_buildfiles:
      logger.debug('BuildFile %s has already been parsed from another address.' % buildfile)
      return

    assert(buildfile not in self._addresses_by_buildfile,
           'BUILD file %s already found in BuildGraph._addresses_by_buildfile even though this'
           ' BUILD file has already been added to the build graph.  The addresses are: %s' % 
           (buildfile, self._addresses_by_buildfile[buildfile]))

    # Since this is a potentially expensive operation, we must explicitly call parse() rather than
    # rely on the BUILD file to implicitly parse when we request the targets and addresses it
    # exports.
    buildfile.parse()

    for address, target in buildfile.targets_by_address.items():
      logger.debug('Adding target %s to the build graph with address %s' %
                   (target, address))

      assert(address not in self._targets_by_address,
             'Address %s already in BuildGraph._targets_by_address even though this BUILD file has'
             ' not yet been added to the BuildGraph.  The target is: %s' %
             (address, target))

      assert(address not in self._addresses_by_buildfile[buildfile],
             'Address %s has already been associated with buildfile %s in the build graph.' % 
             (address, self._addresses_by_buildfile[buildfile]))

      self._targets_by_address[address] = target
      self._addresses_by_buildfile[buildfile].add(address)

    self._added_buildfiles.add(buildfile)
    logger.debug('buildfile %s successfully added to the build graph.' % buildfile)

