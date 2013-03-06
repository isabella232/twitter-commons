
from collections import defaultdict

class AggregateTimings(object):
  """Aggregates timings over multiple invocations of 'similar' work.

  Useful for finding bottlenecks.
  """
  def __init__(self):
    # Map path -> timing in seconds (a float)
    self._timings_by_path = defaultdict(float)

  def add_timing(self, label, secs):
    """Aggregate timings by label."""
    self._timings_by_path[label] += secs

  def get_all(self):
    """Returns all the timings, sorted in decreasing order.

    Each value is a dict: { path: <path>, timing: <timing in seconds> }
    """
    return [{ 'path': x[0], 'timing': x[1]}
            for x in sorted(self._timings_by_path.items(), key=lambda x: x[1], reverse=True)]
