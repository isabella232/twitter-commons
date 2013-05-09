
import os
import sys
import time

from contextlib import contextmanager

from twitter.pants.goal.artifact_cache_stats import ArtifactCacheStats
from twitter.pants.goal.default_reporting import default_reporting
from twitter.pants.base.run_info import RunInfo
from twitter.pants.goal.aggregated_timings import AggregatedTimings
from twitter.pants.goal.workunit import WorkUnit


class RunTracker(object):
  """Tracks and times the execution of a pants run."""
  def __init__(self, config):
    self.run_timestamp = time.time()  # A double, so we get subsecond precision for ids.
    cmd_line = ' '.join(['pants'] + sys.argv[1:])

    # run_id is safe for use in paths.
    millis = (self.run_timestamp * 1000) % 1000
    run_id = 'pants_run_%s_%d' % \
             (time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime(self.run_timestamp)), millis)

    self.info_dir = os.path.join(config.getdefault('info_dir'), run_id)
    self.run_info = RunInfo(os.path.join(self.info_dir, 'info'))
    self.run_info.add_basic_info(run_id, self.run_timestamp)
    self.run_info.add_info('cmd_line', cmd_line)

    # Create a 'latest' symlink, after we add_infos, so we're guaranteed that the file exists.
    link_to_latest = os.path.join(os.path.dirname(self.info_dir), 'latest')
    if os.path.exists(link_to_latest):
      os.unlink(link_to_latest)
    os.symlink(self.info_dir, link_to_latest)

    # Time spent in a workunit, including its children.
    self.cumulative_timings = AggregatedTimings(os.path.join(self.info_dir, 'cumulative_timings'))

    # Time spent in a workunit, not including its children.
    self.self_timings = AggregatedTimings(os.path.join(self.info_dir, 'self_timings'))

    self.artifact_cache_stats = \
      ArtifactCacheStats(os.path.join(self.info_dir, 'artifact_cache_stats'))

    self.report = default_reporting(config, self)
    self.report.open()

    self.root_workunit = WorkUnit(run_tracker=self, parent=None,
                                  labels=[], name='all', cmd=None)
    self.root_workunit.start()
    self.report.start_workunit(self.root_workunit)
    self._current_workunit = self.root_workunit

    self.options = None  # Set later, after options are parsed.

  def close(self):
    while self._current_workunit:
      self.report.end_workunit(self._current_workunit)
      self._current_workunit.end()
      self._current_workunit = self._current_workunit.parent
    self.report.close()
    try:
      self.run_info.add_info('outcome', self.root_workunit.outcome_string())
    except IOError:
      pass  # If the goal is clean-all then the run info dir no longer exists...

  def current_workunit(self):
    return self._current_workunit

  @contextmanager
  def new_workunit(self, name, labels=list(), cmd=''):
    """Creates a (hierarchical) subunit of work for the purpose of timing and reporting.

    - name: A short name for this work. E.g., 'resolve', 'compile', 'scala', 'zinc'.
    - labels: An optional iterable of labels. The reporters can use this to decide how to
              display information about this work.
    - cmd: An optional longer string representing this work.
           E.g., the cmd line of a compiler invocation.

    Use like this:

    with context.new_workunit(name='compile', labels=[WorkUnit.GOAL]) as workunit:
      <do scoped work here>
      <set the outcome on workunit if necessary>

    Note that the outcome will automatically be set to failure if an exception is raised
    in a workunit, and to success otherwise, so often you only need to set the
    outcome explicitly if you want to set it to warning.
    """
    self._current_workunit = WorkUnit(run_tracker=self, parent=self._current_workunit,
                                      name=name, labels=labels, cmd=cmd)
    self._current_workunit.start()
    try:
      self.report.start_workunit(self._current_workunit)
      yield self._current_workunit
    except KeyboardInterrupt:
      self._current_workunit.set_outcome(WorkUnit.ABORTED)
      raise
    except:
      self._current_workunit.set_outcome(WorkUnit.FAILURE)
      raise
    else:
      self._current_workunit.set_outcome(WorkUnit.SUCCESS)
    finally:
      self.report.end_workunit(self._current_workunit)
      self._current_workunit.end()
      self._current_workunit = self._current_workunit.parent
