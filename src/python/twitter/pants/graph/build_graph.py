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

  def __init__(self, build_root, build_file_parser):
    self._build_root = build_root
    self._build_file_parser = build_file_parser
    self._targets_by_address = {}
    self._addresses_by_build_file = defaultdict(OrderedSet)
    self._added_build_files = set()

  def add_spec(self, spec):
    '''
    Add a spec (a string) to the build graph.  This method should be idempotent for equivalent
    specs
    '''

    address = Address.parse(self._build_root, spec)
    self.add_address(address)

  def add_address(self, address):
    '''
    Add an Address object to the build graph by parsing its BUILD file.  This method should be
    idempotent for equivalent addresses (and in fact for any set of addresses that live in the
    same BUILD file.)
    '''

    logger.debug('Adding Address %s to the build graph.' % address)

    build_file = address.build_file

    if address in self._targets_by_address:
      logger.debug('Address %s already added to the build graph.')
      assert build_file in self._added_build_files, (
        '{address} from {spec} has been added to the BuildGraph, but no build_file was'
        ' found in _added_build_files.'
        .format(address=address, spec=spec))
      return

    if build_file in self._added_build_files:
      logger.debug('BuildFile %s has already been parsed from another address.' % build_file)
      return

    assert build_file not in self._addresses_by_build_file, (
      '{build_file} already found in BuildGraph._addresses_by_build_file even though this'
      ' BUILD file has already been added to the build graph.  The addresses are: {addresses}'
      .format(build_file=build_file, addresses=self._addresses_by_build_file[build_file]))

    parsed_target_proxies = self._build_file_parser.parse_build_file(build_file)

    for target_proxy in parsed_target_proxies:
      address = Address(buld_file, target_proxy.name)
      logger.debug('Adding {target_proxy} to the build graph with {address}'
                   .format(target_proxy=target_proxy,
                           address=address))

      assert address not in self._targets_by_address, (
        '{address} already in BuildGraph._targets_by_address even though this BUILD file has'
        ' not yet been added to the BuildGraph.  The target type is: {target_type}' %
        (address, target))

      assert address not in self._addresses_by_build_file[build_file], (
        '{address} has already been associated with {build_file} in the build graph.'
        .format(address=address,
                build_file=self._addresses_by_build_file[build_file])
      )

      self._targets_by_address[address] = target_proxy
      self._addresses_by_build_file[build_file].add(address)

    self._added_build_files.add(build_file)
    logger.debug('build_file %s successfully added to the build graph.' % build_file)
