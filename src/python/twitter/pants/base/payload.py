
from hashlib import sha1
import os

from twitter.common.collections import OrderedSet
from twitter.common.lang import AbstractClass

from twitter.pants.base.build_environment import get_buildroot


def hash_sources(hasher, root_path, rel_path, sources):
  hasher.update(rel_path)
  for source in sources:
    with open(os.path.join(root_path, rel_path, source), 'r') as f:
      hasher.update(source)
      hasher.update(f.read())


class Payload(AbstractClass):
  def invalidation_hash(hasher):
    raise NotImplemented

  def has_sources(self, extension):
    raise NotImplemented

  def has_resources(self, extension):
    raise NotImplemented


class JvmTargetPayload(Payload):
  def __init__(self,
               sources_rel_path=None,
               sources=None,
               provides=None,
               excludes=None,
               configurations=None):
    self.sources_rel_path = sources_rel_path
    self.sources = OrderedSet(sources)
    self.provides = provides
    self.excludes = OrderedSet(excludes)
    self.configurations = OrderedSet(configurations)

  def __hash__(self):
    return hash((self.sources, self.provides, self.excludes, self.configurations))

  def has_sources(self, extension=''):
    return any(source.endswith(extension) for source in self.sources)

  def has_resources(self):
    return False

  def sources_relative_to_buildroot(self):
    return [os.path.join(self.sources_rel_path, source) for source in self.sources]

  def invalidation_hash(self, hasher):
    sources_hash = hash_sources(hasher, get_buildroot(), self.sources_rel_path, self.sources)
    hasher.update(str(hash(self.provides)))
    for exclude in self.excludes:
      hasher.update(str(hash(exclude)))
    for config in self.configurations:
      hasher.update(config)

class JarLibraryPayload(Payload):
  def __init__(self, jars, overrides):
    self.jars = OrderedSet(jars)
    self.overrides = OrderedSet(overrides)

  def has_sources(self, extension):
    return False

  def has_resources(self):
    return False

  def invalidation_hash(self, hasher):
    hasher.update(str(hash(self.jars)))
    hasher.update(str(hash(self.overrides)))
