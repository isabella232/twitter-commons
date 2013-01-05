import os
import threading

from contextlib import contextmanager
from twitter.common.dirutil import safe_rmtree, safe_mkdir

from twitter.common.lang import Compatibility
from twitter.common.threading import PeriodicThread
from twitter.pants.reporting.formatter import HTMLFormatter, PlainTextFormatter
from twitter.pants.reporting.reporter import ConsoleReporter, FileReporter

StringIO = Compatibility.StringIO



def default_reporting(context):
  reports_dir = context.config.get('reporting', 'reports_dir')
  link_to_latest = os.path.join(reports_dir, 'latest')
  if os.path.exists(link_to_latest):
    os.unlink(link_to_latest)

  run_id = context.run_info.get_info('id')
  if run_id is None:
    raise Exception, 'No run_id set'
  this_run_dir = os.path.join(reports_dir, run_id)
  safe_rmtree(this_run_dir)
  context.run_info.add_info('default_report', this_run_dir)

  this_run_html_dir = os.path.join(this_run_dir, 'html')
  safe_mkdir(this_run_html_dir)
  os.symlink(this_run_dir, link_to_latest)

  assets_dir = context.config.get('reporting', 'reports_assets_dir')
  os.symlink(assets_dir, os.path.join(this_run_dir, 'assets'))

  html_output_path = os.path.join(this_run_html_dir, 'build.html')

  report = Report()
  report.add_reporter(ConsoleReporter(PlainTextFormatter()))
  template_dir = context.config.get('reporting', 'reports_template_dir')
  report.add_reporter(FileReporter(HTMLFormatter(template_dir), html_output_path))
  return report

class Report(object):
  class Outcome:
    def __init__(self):
      self.status = Report.FAILURE  # One of Report.{FAILURE,SUCCESS,WARNING}

  FAILURE = 0
  SUCCESS = 1
  WARNING = 2

  def __init__(self):
    # We periodically emit newly reported data.
    self._emitter_thread = PeriodicThread(target=self._lock_and_notify, name='report-emitter', period_secs=0.1)
    self._emitter_thread.daemon = True

    # Our read-write buffer.
    self._lock = threading.Lock()
    self._io = StringIO()
    self._readpos = 0
    self._writepos = 0

    # We report to these reporters.
    self._reporters = []

    # Nested report scopes.
    self._scopes = []

  def open(self):
    with self._lock:
      for reporter in self._reporters:
        reporter.open()
    self._emitter_thread.start()

  def add_reporter(self, reporter):
    with self._lock:
      self._reporters.append(reporter)

  @contextmanager
  def scope(self, scope):
    self.enter_scope(scope)
    outcome = Report.Outcome()  # Caller sets fields here to convey outcome.
    yield outcome
    self.exit_scope(outcome)

  def enter_scope(self, scope):
    self._scopes.append(scope)
    with self._lock:
      self._notify()  # Make sure we flush everything reported until now.
      for reporter in self._reporters:
        reporter.enter_scope(self._scopes)

  def exit_scope(self, outcome):
    with self._lock:
      self._notify()  # Make sure we flush everything reported until now.
      for reporter in self._reporters:
        reporter.exit_scope(self._scopes, outcome)
    self._scopes.pop()

  def write(self, s):
    with self._lock:
      self._io.seek(self._writepos)
      self._io.write(str(s))
      self._io.flush()
      self._writepos = self._io.tell()

  def write_targets(self, prefix, targets):
    indent = '\n' + ' ' * (len(prefix) + 1)
    s = '%s %s\n' % (prefix, indent.join([t.address.reference() for t in targets]))
    self.write(s)

  def flush(self):
    pass

  def close(self):
    self._emitter_thread.stop()
    with self._lock:
      for reporter in self._reporters:
        reporter.close()

  def _read(self):
    self._io.seek(self._readpos)
    ret = self._io.read()
    self._readpos = self._io.tell()
    return ret

  def _lock_and_notify(self):
    with self._lock:
      self._notify()

  def _notify(self):
    s = self._read()
    if len(s) > 0:
      for reporter in self._reporters:
        reporter.handle_output(s)
