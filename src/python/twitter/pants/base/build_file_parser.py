from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)

from collections import defaultdict
from copy import deepcopy
from functools import partial
import os.path

from twitter.common.python import compatibility

from twitter.pants.base.address import BuildFileAddress, SyntheticAddress
from twitter.pants.base.build_file import BuildFile

import logging
logger = logging.getLogger(__name__)


class TargetProxy(object):
  def __init__(self, target_type, build_file, args, kwargs):
    # Deep copy in case someone is being too tricky for their own good in their BUILD files.
    kwargs = deepcopy(kwargs)

    assert 'name' in kwargs, (
      'name is a required parameter to all Target objects specified within a BUILD file.'
      '  Target type was: {target_type}.'
      '  Current BUILD file is: {build_file}.'
      .format(target_type=target_type,
              build_file=build_file))

    assert not args, (
      'All arguments passed to Targets within BUILD files should use explicit keyword syntax.'
      '  Target type was: {target_type}.'
      '  Current BUILD file is: {build_file}.'
      '  Arguments passed were: {args}'
      .format(target_type=target_type,
              build_file=build_file,
              args=args))

    assert 'build_file' not in kwargs, (
      'build_file cannot be passed as an explicit argument to a target within a BUILD file.'
      '  Target type was: {target_type}.'
      '  Current BUILD file is: {build_file}.'
      '  build_file argument passed was: {build_file_arg}'
      .format(target_type=target_type,
              build_file=build_file,
              build_file_arg=kwargs.get('build_file')))

    self.target_type = target_type
    self.build_file = build_file
    self.kwargs = kwargs
    self.name = kwargs['name']
    self.address = BuildFileAddress(build_file, self.name)

  @property
  def dependencies(self):
    return self.kwargs.get('dependencies', [])

  def __str__(self):
    format_str = ('<TargetProxy(target_type={target_type}, build_file={build_file})'
                  ' [name={name}, address={address}]>')
    return format_str.format(target_type=self.target_type,
                             build_file=self.build_file,
                             name=self.name,
                             address=self.address)

  def __repr__(self):
    format_str = 'TargetProxy(target_type={target_type}, build_file={build_file}, kwargs={kwargs})'
    return format_str.format(target_type=self.target_type,
                             build_file=self.build_file,
                             kwargs=self.kwargs)


class TargetCallProxy(object):
  def __init__(self, target_type, build_file, registered_target_proxies):
    self._target_type = target_type
    self._build_file = build_file
    self._registered_target_proxies = registered_target_proxies

  def __call__(self, *args, **kwargs):
    target_proxy = TargetProxy(self._target_type, self._build_file, args, kwargs)
    self._registered_target_proxies.add(target_proxy)

  def __repr__(self):
    return ('<TargetCallProxy(target_type={target_type}, build_file={build_file},'
            ' registered_target_proxies=<dict with id: {registered_target_proxies_id}>)>'
            .format(target_type=self._target_type,
                    build_file=self._build_file,
                    registered_target_proxies=id(self._registered_target_proxies)))


class BuildFileParser(object):
  def __init__(self, root_dir, exposed_objects, path_relative_utils, target_alias_map):
    self._root_dir = root_dir
    self._exposed_objects = exposed_objects
    self._path_relative_utils = path_relative_utils
    self._target_alias_map = target_alias_map

    self._target_proxy_by_address = {}
    self._target_proxies_by_build_file = defaultdict(set)
    self._addresses_by_build_file = defaultdict(set)
    self._added_build_files = set()

  def parse_build_file(self, build_file):
    if build_file in self._added_build_files:
      logger.debug('BuildFile %s has already been parsed.' % build_file)

    logger.debug("Parsing BUILD file %s." % build_file)
    with open(build_file.full_path, 'r') as build_file_fp:
      build_file_bytes = build_file_fp.read()

    parse_context = {}
    parse_context.update(self._exposed_objects)
    parse_context.update(dict((
      (key, partial(util, rel_path=os.path.dirname(build_file.relpath))) for 
      key, util in self._path_relative_utils.items()
    )))
    registered_target_proxies = set()
    parse_context.update(dict((
      (alias, TargetCallProxy(target_type=target_type,
                              build_file=build_file,
                              registered_target_proxies=registered_target_proxies)) for
      alias, target_type in self._target_alias_map.items()
    )))

    try:
      build_file_code = build_file.code()
    except Exception as e:
      logger.error("Error parsing {build_file}.  Exception was:\n {exception}"
                   .format(build_file=build_file, exception=e))
      raise e

    try:
      compatibility.exec_function(build_file_code, parse_context)
    except Exception as e:
      logger.error("Error running {build_file}.  Exception was:\n {exception}"
                   .format(build_file=build_file, exception=e))
      raise e

    for target_proxy in registered_target_proxies:
      logger.debug('Adding {target_proxy} to the proxy build graph with {address}'
                   .format(target_proxy=target_proxy,
                           address=target_proxy.address))

      assert target_proxy.address not in self._target_proxy_by_address, (
        '{address} already in BuildGraph._targets_by_address even though this BUILD file has'
        ' not yet been added to the BuildGraph.  The target type is: {target_type}'
        .format(address=target_proxy.address,
                target_type=target_proxy.target_type))

      assert target_proxy.address not in self._addresses_by_build_file[build_file], (
        '{address} has already been associated with {build_file} in the build graph.'
        .format(address=target_proxy.address,
                build_file=self._addresses_by_build_file[build_file])
      )

      self._target_proxy_by_address[target_proxy.address] = target_proxy
      self._addresses_by_build_file[build_file].add(target_proxy.address)
      self._target_proxies_by_build_file[build_file].add(target_proxy)
    self._added_build_files.add(build_file)

    logger.debug("{build_file} produced the following TargetProxies:"
                 .format(build_file=build_file))
    for target_proxy in registered_target_proxies:
      logger.debug("  * {target_proxy}".format(target_proxy=target_proxy))

  def add_build_file_spec(self, spec):
    build_file_relpath = SyntheticAddress(spec).spec_path
    build_file = BuildFile(root_dir=self._root_dir, relpath=build_file_relpath)

    self.parse_build_file(build_file)
    target_proxies = self._target_proxies_by_build_file[build_file]

    logger.debug('{build_file} successfully added to the proxy build graph.'
                 .format(build_file=build_file))

    logger.debug('Recursively parsing transitively referenced BUILD files of all TargetProxies'
                 ' in {build_file}'.format(build_file=build_file))
    for target_proxy in target_proxies:
      for dependency_spec in target_proxy.dependencies:
        if dependency_spec.startswith(':'):
          logger.debug('Skipping relative dependency spec {dependency_spec} for {target_proxy})'
                       .format(dependency_spec=dependency_spec,
                               target_proxy=target_proxy))
        else:
          logger.debug('Recursively parsing dependency spec {dependency_spec} for {target_proxy}'
                       .format(dependency_spec=dependency_spec,
                               target_proxy=target_proxy))
          dep_build_file_relpath = SyntheticAddress(dependency_spec).spec_path
          dep_build_file = BuildFile(root_dir=self._root_dir, relpath=dep_build_file_relpath)
          if not dep_build_file in self._added_build_files:
            # Pretty sure this always means there's a cycle in the graph.
            self.add_build_file_spec(dependency_spec)
      logger.debug('Finished recursively parsing dependency specs for {target_proxy}'
                   .format(target_proxy=target_proxy))
    logger.debug('Finished Recursively parsing transitively referenced BUILD files of all'
                 ' TargetProxies in {build_file}'
                 .format(build_file=build_file))

  def add_address(self, address):
    '''
    Add an Address object to the build graph by parsing its BUILD file.  This method should be
    idempotent for equivalent addresses (and in fact for any set of addresses that live in the
    same BUILD file.)
    '''

    logger.debug('Adding Address %s to the build graph.' % address)

    build_file = address.build_file

    # if address in self._targets_by_address:
    #   logger.debug('Address %s already added to the build graph.')
    #   assert build_file in self._added_build_files, (
    #     '{address} from {spec} has been added to the BuildGraph, but no build_file was'
    #     ' found in _added_build_files.'
    #     .format(address=address, spec=spec))
    #   return



    # assert build_file not in self._addresses_by_build_file, (
    #   '{build_file} already found in BuildGraph._addresses_by_build_file even though this'
    #   ' BUILD file has already been added to the build graph.  The addresses are: {addresses}'
    #   .format(build_file=build_file, addresses=self._addresses_by_build_file[build_file]))

    parsed_target_proxies = self._build_file_parser.parse_build_file(build_file)

