
from hashlib import sha1
import os

from twitter.common.collections import OrderedSet
from twitter.common.lang import AbstractClass

from twitter.pants.base.build_environment import get_buildroot


def hash_sources(root_path, rel_path, sources):
  hasher = sha1()
  hasher.update(rel_path)
  for source in sources:
    with open(os.path.join(root_path, rel_path, source), 'r') as f:
      hasher.update(source)
      hasher.update(f.read())
  return hasher.hexdigest()


class Payload(AbstractClass):
  pass


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

  def invalidation_hash(self):
    sources_hash = hash_sources(get_buildroot(), self.sources_rel_path, self.sources)
    hasher = sha1()
    hasher.update(sources_hash)
    hasher.update(str(hash(self.provides)))
    for exclude in self.excludes:
      hasher.update(str(hash(exclude)))
    for config in self.configurations:
      hasher.update(config)
    return hasher.hexdigest()