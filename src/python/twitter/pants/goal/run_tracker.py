
import os
import sys
import time

from contextlib import contextmanager

from twitter.pants.goal.artifact_cache_stats import ArtifactCacheStats
from twitter.pants.goal.run_info import RunInfo
from twitter.pants.goal.aggregated_timings import AggregatedTimings
from twitter.pants.goal.work_unit import WorkUnit
from twitter.pants.reporting.report import default_reporting


class RunTracker(object):
  """Tracks and times the execution of a pants run."""
  def __init__(self, config):
    run_timestamp = time.time()
    # run_id is safe for use in paths.
    millis = (run_timestamp * 1000) % 1000
    run_id = 'pants_run_%s_%d' %\
             (time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime(run_timestamp)), millis)
    cmd_line = ' '.join(['pants'] + sys.argv[1:])
    info_dir = config.getdefault('info_dir')
    self.run_info = RunInfo(os.path.join(info_dir, '%s.info' % run_id))
    self.run_info.add_infos([('id', run_id), ('timestamp', run_timestamp), ('cmd_line', cmd_line)])
    # Create a 'latest' symlink, after we add_infos, so we're guaranteed that the file exists.
    link_to_latest = os.path.join(info_dir, 'latest.info')
    if os.path.exists(link_to_latest):
      os.unlink(link_to_latest)
    os.symlink(self.run_info.path(), link_to_latest)

    # Time spent in a workunit, including its children.
    self.cumulative_timings = AggregatedTimings()

    # Time spent in a workunit, not including its children.
    self.self_timings = AggregatedTimings()

    self.artifact_cache_stats = ArtifactCacheStats()

    self.report = default_reporting(config, self)
    self.report.open()

    self.root_workunit = WorkUnit(run_tracker=self, parent=None,
                                  types=[], name='all', cmd=None)
    self.root_workunit.start()
    self.report.start_workunit(self.root_workunit)
    self._current_workunit = self.root_workunit

    self.options = None  # Set later, after options are parsed.

  def close(self):
    while self._current_workunit:
      self._current_workunit.end()
      self.report.end_workunit(self._current_workunit)
      self._current_workunit = self._current_workunit.parent
    self.report.close()
    try:
      self.run_info.add_info('outcome', self.root_workunit.outcome_string())
    except IOError:
      pass  # If the goal is clean-all then the run info dir no longer exists...

  def current_work_unit(self):
    return self._current_workunit

  @contextmanager
  def new_workunit(self, name, types=list(), cmd=''):
    """Creates a (hierarchical) subunit of work for the purpose of timing and reporting.

    - name: A short name for this work. E.g., 'resolve', 'compile', 'scala', 'zinc'.
    - types: An optional iterable of types. The reporters can use this to decide how to
             display information about this work.
    - cmd: An optional longer string representing this work.
           E.g., the cmd line of a compiler invocation.

    Use like this:

    with context.new_workunit(name='compile', types=[WorkUnit.GOAL]) as workunit:
      <do scoped work here>
      <set the outcome on workunit if necessary>

    Note that the outcome will automatically be set to failure if an exception is raised
    in a workunit, and to success otherwise, so often you only need to set the
    outcome explicitly if you want to set it to warning.
    """
    self._current_workunit = WorkUnit(run_tracker=self, parent=self._current_workunit,
                                      name=name, types=types, cmd=cmd)
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
      self._current_workunit.end()
      self.report.end_workunit(self._current_workunit)
      self._current_workunit = self._current_workunit.parent
