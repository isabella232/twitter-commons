import os
import threading

from twitter.common.dirutil import safe_rmtree, safe_mkdir

from twitter.common.lang import Compatibility
from twitter.common.threading import PeriodicThread
from twitter.pants.reporting.formatter import HTMLFormatter, IndentingPlainTextFormatter
from twitter.pants.reporting.reporter import ConsoleReporter, MultiFileReporter

StringIO = Compatibility.StringIO


def default_reporting(config, run_info):
  reports_dir = config.get('reporting', 'reports_dir')
  link_to_latest = os.path.join(reports_dir, 'latest')
  if os.path.exists(link_to_latest):
    os.unlink(link_to_latest)

  run_id = run_info.get_info('id')
  if run_id is None:
    raise ReportingError('No run_id set')
  this_run_dir = os.path.join(reports_dir, run_id)
  safe_rmtree(this_run_dir)

  this_run_html_dir = os.path.join(this_run_dir, 'html')
  safe_mkdir(this_run_html_dir)
  os.symlink(this_run_dir, link_to_latest)

  html_output_path = os.path.join(this_run_html_dir, 'build.html')
  run_info.add_info('default_report', html_output_path)

  report = Report()
  report.add_reporter(ConsoleReporter(IndentingPlainTextFormatter()))
  template_dir = config.get('reporting', 'reports_template_dir')
  report.add_reporter(
    MultiFileReporter(HTMLFormatter(template_dir, this_run_html_dir), this_run_html_dir))
  return report

class ReportingError(Exception):
  pass

class Report(object):
  """A report of a pants run."""

  def __init__(self):
    # We periodically emit newly reported data.
    self._emitter_thread = \
      PeriodicThread(target=self._lock_and_notify, name='report-emitter', period_secs=0.1)
    self._emitter_thread.daemon = True

    # Map from workunit id to workunit.
    self._workunits = {}

    # We report to these reporters.
    self._reporters = []

    # We synchronize our state on this.
    self._lock = threading.Lock()

  def open(self):
    with self._lock:
      for reporter in self._reporters:
        reporter.open()
    self._emitter_thread.start()

  def add_reporter(self, reporter):
    with self._lock:
      self._reporters.append(reporter)

  def start_workunit(self, workunit):
    with self._lock:
      self._notify()  # Make sure we flush everything reported until now.
      self._workunits[workunit.id] = workunit
      for reporter in self._reporters:
        reporter.start_workunit(workunit)

  def report_targets(self, workunit, parts):
    """Report which targets we're currently working on.

    Argument is list of target partitions. Each partition is a list of pairs
    (target address, number of source files).
    """
    with self._lock:
      self._notify()  # Make sure we flush everything reported until now.
      for reporter in self._reporters:
        reporter.report_targets(workunit, parts)

  def message(self, workunit, s):
    """Report a message."""
    with self._lock:
      self._notify()  # Make sure we flush everything reported until now.
      for reporter in self._reporters:
        reporter.handle_message(workunit, s)

  def end_workunit(self, workunit):
    with self._lock:
      self._notify()  # Make sure we flush everything reported until now.
      for reporter in self._reporters:
        reporter.end_workunit(workunit)
      del self._workunits[workunit.id]

  def close(self):
    self._emitter_thread.stop()
    with self._lock:
      for reporter in self._reporters:
        reporter.close()

  def _lock_and_notify(self):
    with self._lock:
      self._notify()

  def _notify(self):
    # Notify for output in all workunits. Note that output may be coming in from workunits other
    # than the current one, if work is happening in parallel.
    for workunit in self._workunits.values():
      for label, output in workunit.outputs().items():
        s = output.read()
        if len(s) > 0:
          for reporter in self._reporters:
            reporter.handle_output(workunit, label, s)
