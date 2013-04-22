
import os
import sys
import time

from contextlib import contextmanager
from twitter.common.dirutil import safe_mkdir, safe_rmtree

from twitter.pants.base.build_info import get_build_info
from twitter.pants.goal.artifact_cache_stats import ArtifactCacheStats
from twitter.pants.goal.run_info import RunInfo
from twitter.pants.goal.aggregated_timings import AggregatedTimings
from twitter.pants.goal.work_unit import WorkUnit
from twitter.pants.reporting.console_reporter import ConsoleReporter
from twitter.pants.reporting.html_reporter import HtmlReporter
from twitter.pants.reporting.report import ReportingError, Report


def default_reporting(config, run_tracker):
  """Sets up the default reporting configuration."""
  reports_dir = config.get('reporting', 'reports_dir')
  link_to_latest = os.path.join(reports_dir, 'latest')
  if os.path.exists(link_to_latest):
    os.unlink(link_to_latest)

  run_id = run_tracker.run_info.get_info('id')
  if run_id is None:
    raise ReportingError('No run_id set')
  run_dir = os.path.join(reports_dir, run_id)
  safe_rmtree(run_dir)

  html_dir = os.path.join(run_dir, 'html')
  safe_mkdir(html_dir)
  os.symlink(run_dir, link_to_latest)

  report = Report()

  console_reporter = ConsoleReporter(run_tracker, indenting=True)
  template_dir = config.get('reporting', 'reports_template_dir')
  html_reporter = HtmlReporter(run_tracker, html_dir, template_dir)

  report.add_reporter(console_reporter)
  report.add_reporter(html_reporter)

  run_tracker.run_info.add_info('default_report', html_reporter.report_path())

  return report


class RunTracker(object):
  """Tracks and times the execution of a pants run."""
  def __init__(self, config):
    bi = get_build_info()
    run_timestamp = bi.epochtime
    # run_id is safe for use in paths.
    millis = (run_timestamp * 1000) % 1000
    run_id = 'pants_run_%s_%d' %\
             (time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime(run_timestamp)), millis)
    cmd_line = ' '.join(['pants'] + sys.argv[1:])

    self.info_dir = os.path.join(config.getdefault('info_dir'), run_id)
    self.run_info = RunInfo(os.path.join(self.info_dir, 'info'))
    self.run_info.add_infos([
      ('id', run_id), ('timestamp', run_timestamp), ('cmd_line', cmd_line),
      ('branch', bi.branch), ('tag', bi.tag), ('sha', bi.sha), ('name', bi.name),
      ('machine', bi.machine), ('buildroot', bi.path)])

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
                                  types=[], name='all', cmd=None)
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
      self.report.end_workunit(self._current_workunit)
      self._current_workunit.end()
      self._current_workunit = self._current_workunit.parent
