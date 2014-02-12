

class RegisterableTargetProxy(object):
  def __init__(self, target_type, registered_targets):
    self.target_type = target_type
    self.registered_targets = registered_targets

  def __call__(self, *args, **kwargs):
    target = self.target_type(*args, **kwargs)
    self.registered_targets.add(target)


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
    registered_targets = set()
    parse_context.update({
      alias: RegisterableTargetProxy(target_type, registered_targets) for
      alias, target_type in self.target_alias_map.items()
    })

    try:
      build_file_code = compile(build_file_bytes, '<string>', 'exec', flags=0, dont_inherit=True)
    except Exception as e:
      logger.error("Error parsing BUILD file %s.  Exception was:\n %s" % (build_file, e))
      raise e

    try:
      compatibility.exec_function(build_file_code, globals=parse_context)
    except Exception as e:
      logger.error("Error running BUILD file %s.  Exception was:\n %s" % (build_file, e))

    logger.debug("BUILD file %s produced the following Targets:" % build_file)
    for target in registered_targets:
      logger.debug("  * %s" % target)

    return registered_targets

