from collections import defaultdict

from twitter.common.dirutil import safe_mkdir_for


class AggregatedTimings(object):
  """Aggregates timings over multiple invocations of 'similar' work.

  If filepath is not none, stores the timings in that file. Useful for finding bottlenecks."""
  def __init__(self, path=None):
    # Map path -> timing in seconds (a float)
    self._timings_by_path = defaultdict(float)
    self._tool_labels = set()
    self._path = path
    safe_mkdir_for(self._path)

  def add_timing(self, label, secs, is_tool=False):
    """Aggregate timings by label."""
    self._timings_by_path[label] += secs
    if is_tool:
      self._tool_labels.add(label)
    if self._path:
      with open(self._path, 'w') as f:
        for x in self.get_all():
          f.write('%(label)s: %(timing)s\n' % x)

  def get_all(self):
    """Returns all the timings, sorted in decreasing order.

    Each value is a dict: { path: <path>, timing: <timing in seconds> }
    """
    return [{ 'label': x[0], 'timing': x[1], 'is_tool': x[0] in self._tool_labels}
            for x in sorted(self._timings_by_path.items(), key=lambda x: x[1], reverse=True)]
