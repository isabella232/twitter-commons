from __future__ import (nested_scopes, generators, division, absolute_import, with_statement,
                        print_function, unicode_literals)


from copy import deepcopy
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
      .format(target_type=self._target_type,
              build_file=self._build_file))

    assert not args, (
      'All arguments passed to Targets within BUILD files should use explicit keyword syntax.'
      '  Target type was: {target_type}.'
      '  Current BUILD file is: {build_file}.'
      '  Arguments passed were: {args}'
      .format(target_type=self._target_type,
              build_file=self._build_file,
              args=args))

    assert 'build_file' not in kwargs, (
      'build_file cannot be passed as an explicit argument to a target within a BUILD file.'
      '  Target type was: {target_type}.'
      '  Current BUILD file is: {build_file}.'
      '  build_file argument passed was: {build_file_arg}'
      .format(target_type=self._target_type,
              build_file=self._build_file,
              build_file_arg=kwargs.get('build_file')))

    self._target_type = target_type
    self._build_file = build_file
    self._kwargs = kwargs
    self.name = kwargs['name']
    self.address = Address(build_file, self.name)

  def __str__(self):
    format_str = ('<TargetProxy(target_type={target_type}, build_file={build_file})'
                  ' [name={name}, address={address}]>')
    return format_str.format(target_type=self._target_type,
                             build_file=self._build_file,
                             name=self.name,
                             address=self.address)

  def __repr__(self):
    format_str = 'TargetProxy(target_type={target_type}, build_file={build_file}, kwargs={kwargs})'
    return format_str.format(target_type=self._target_type,
                             build_file=self._build_file,
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
  def __init__(self, exposed_objects, path_relative_utils, target_alias_map):
    self.exposed_objects = exposed_objects
    self.path_relative_utils = path_relative_utils
    self.target_alias_map = target_alias_map

  def parse_build_file(build_file):
    logger.debug("Parsing BUILD file %s." % build_file)
    with open(build_file.full_path, 'r') as build_file_fp:
      build_file_bytes = build_file_fp.read()

    parse_context = {}
    parse_context.update(exposed_objects)
    parse_context.update({
      key: partial(util, rel_path=build_file.rel_path) for 
      key, util in self.path_relative_utils.items()
    })
    registered_target_proxies = set()
    parse_context.update({
      alias: TargetCallProxy(target_type=target_type,
                             build_file=build_file,
                             registered_target_proxies=registered_target_proxies) for
      alias, target_type in self.target_alias_map.items()
    })

    try:
      build_file_code = compile(build_file_bytes, '<string>', 'exec', flags=0, dont_inherit=True)
    except Exception as e:
      logger.error("Error parsing {build_file}.  Exception was:\n {exception}"
                   .format(build_file=build_file, exception=e))
      raise e

    try:
      compatibility.exec_function(build_file_code, globals=parse_context)
    except Exception as e:
      logger.error("Error running {build_file}.  Exception was:\n {exception}"
                   .format(build_file=build_file, exception=e))
      raise e

    logger.debug("{build_file} produced the following TargetProxies:"
                 .format(build_file=build_file))
    for target_proxy in registered_target_proxies:
      logger.debug("  * {target_proxy}".format(target_proxy=target_proxy))

    return registered_target_proxies

