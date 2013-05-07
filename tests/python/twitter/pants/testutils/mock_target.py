from twitter.pants.base import ParseContext

__author__ = 'Ryan Williams'

from collections import defaultdict
from twitter.pants.targets import InternalTarget, TargetWithSources


class MockTarget(InternalTarget, TargetWithSources):
  def __init__(self, name, dependencies=None, num_sources=0,
              exclusives={}):
    with ParseContext.temp():
      InternalTarget.__init__(self, name, dependencies)
      TargetWithSources.__init__(self, name)
      self.declared_exclusives = defaultdict(set)
      for k in exclusives:
        self.declared_exclusives[k] = set(exclusives[k])
      self.exclusives = None
    self.num_sources = num_sources

  def resolve(self):
    yield self

  def walk(self, work, predicate=None):
    work(self)
    for dep in self.dependencies:
      dep.walk(work)

