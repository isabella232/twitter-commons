
import os
from collections import defaultdict, namedtuple

from twitter.common.dirutil import safe_mkdir


# Lists of target addresses.
CacheStat = namedtuple('CacheStat', ['hit_targets', 'miss_targets'])

class ArtifactCacheStats(object):
  """Tracks the hits and misses in the artifact cache.

  If dir is specified, writes the hits and misses to files in that dir."""
  def __init__(self, dir=None):
    def init_stat():
      return CacheStat([],[])
    self.stats_per_cache = defaultdict(init_stat)
    self._dir = dir
    safe_mkdir(self._dir)

  def add_hit(self, cache_name, tgt):
    self.stats_per_cache[cache_name].hit_targets.append(tgt.address.reference())
    if self._dir and os.path.exists(self._dir):  # Check existence in case of a clean-all.
      with open(os.path.join(self._dir, '%s.hits' % cache_name), 'a') as f:
        f.write(tgt.address.reference())
        f.write('\n')

  def add_miss(self, cache_name, tgt):
    self.stats_per_cache[cache_name].miss_targets.append(tgt.address.reference())
    if self._dir and os.path.exists(self._dir):  # Check existence in case of a clean-all.
      with open(os.path.join(self._dir, '%s.misses' % cache_name), 'a') as f:
        f.write(tgt.address.reference())
        f.write('\n')

  def get_all(self):
    """Returns the  cache stats as a list of dicts."""
    ret = []
    for cache_name, stat in self.stats_per_cache.items():
      ret.append({
        'cache_name': cache_name,
        'num_hits': len(stat.hit_targets),
        'num_misses': len(stat.miss_targets),
        'hits': stat.hit_targets,
        'misses': stat.miss_targets
      })
    return ret
