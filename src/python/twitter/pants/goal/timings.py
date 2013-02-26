
from collections import defaultdict

class Timings(object):
  """Aggregates timings over multiple invocations of 'similar' work.

  Useful for finding bottlenecks.
  """
  def __init__(self):
    self._timings_by_path = defaultdict(float)

  def add_timing(self, workunit):
    """Aggregate timings by path."""
    self._timings_by_path[workunit.get_path()] += workunit.duration()
