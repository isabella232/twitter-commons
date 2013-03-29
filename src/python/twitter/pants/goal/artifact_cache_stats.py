
from collections import defaultdict, namedtuple


# Lists of target addresses.
CacheStat = namedtuple('CacheStat', ['hit_targets', 'miss_targets'])

class ArtifactCacheStats(object):
  """Tracks the hits and misses in the artifact cache."""
  def __init__(self):
    def init_stat():
      return CacheStat([],[])
    self._stats_per_cache = defaultdict(init_stat)

  def add_hit(self, cache_name, addr):
    self._stats_per_cache[cache_name].hit_targets.append(addr)

  def add_miss(self, cache_name, addr):
    self._stats_per_cache[cache_name].miss_targets.append(addr)

  def get_all(self):
    """Returns the  cache stats."""
    ret = []
    for cache_name, stat in self._stats_per_cache.items():
      ret.append({
        'cache_name': cache_name,
        'num_hits': len(stat.hit_targets),
        'num_misses': len(stat.miss_targets)
      })
    return ret
