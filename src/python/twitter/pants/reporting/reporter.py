import os
import sys

from twitter.common.dirutil import safe_mkdir
from twitter.pants.goal.work_unit import WorkUnit


class Reporter(object):
  def __init__(self, run_tracker, formatter):
    self.run_tracker = run_tracker
    self.formatter = formatter

  def open(self):
    self.handle_formatted(None, None, self.formatter.start_run())

  def close(self):
    self.handle_formatted(None, None, self.formatter.end_run())

  def start_workunit(self, workunit):
    self.handle_formatted(workunit, None, self.formatter.start_workunit(workunit))

  def handle_output(self, workunit, label, s):
    """label - classifies the output e.g., 'stdout' for output captured from a tool's stdout.
    Other labels are possible, e.g., if we capture output from a tool's logfiles.
    """
    self.handle_formatted(workunit, label, self.formatter.format_output(workunit, label, s))

  def handle_message(self, workunit, *msg_elements):
    self.handle_formatted(workunit, None, self.formatter.format_message(workunit, *msg_elements))

  def end_workunit(self, workunit):
    self.handle_formatted(workunit, None, self.formatter.end_workunit(workunit))
    self.overwrite_formatted(None, 'cumulative_timings',
        self.formatter.format_aggregated_timings(self.run_tracker.cumulative_timings))
    self.overwrite_formatted(None, 'self_timings',
        self.formatter.format_aggregated_timings(self.run_tracker.self_timings))
    self.overwrite_formatted(None, 'artifact_cache_stats',
        self.formatter.format_artifact_cache_stats(self.run_tracker.artifact_cache_stats))

  def handle_formatted(self, workunit, label, s):
    raise NotImplementedError('handle_formatted_output() not implemented')

  def overwrite_formatted(self, workunit, label, s):
    raise NotImplementedError('overwrite_formatted_output() not implemented')


class ConsoleReporter(Reporter):
  def close(self):
    if self.run_tracker.options.time:
      print('\n')
      print('Cumulative Timings')
      print('==================')
      print(self.formatter.format_aggregated_timings(self.run_tracker.cumulative_timings))
      print('\n')
      print('Self Timings')
      print('============')
      print(self.formatter.format_aggregated_timings(self.run_tracker.self_timings))
      print('\n')
      print('Artifact Cache Stats')
      print('====================')
      print(self.formatter.format_artifact_cache_stats(self.run_tracker.artifact_cache_stats))
    Reporter.close(self)

  def handle_formatted(self, workunit, label, s):
    if label == WorkUnit.DEFAULT_OUTPUT_LABEL or label is None:
      sys.stdout.write(s)
    # Ignore the other outputs (stdout/stderr of tools etc).

  def overwrite_formatted(self, workunit, label, s):
    # TODO: What does overwriting mean in this context?
    pass


class FileReporter(Reporter):
  """Merges all output, for all labels, into one file."""
  def __init__(self, run_tracker, formatter, path):
    Reporter.__init__(self, run_tracker, formatter)
    self._path = path
    self._file = None

  def open(self):
    safe_mkdir(os.path.dirname(self._path))
    self._file = open(self._path, 'w')
    Reporter.open(self)

  def close(self):
    Reporter.close(self)
    self._file.close()
    self._file = None

  def handle_formatted(self, workunit, label, s):
    self._file.write(s)
    # We must flush in the same thread as the write.
    self._file.flush()

  def overwrite_formatted(self, workunit, label, s):
    # TODO: What does overwriting mean in this context?
    pass


class MultiFileReporter(Reporter):
  """Writes all default output to one file, and all other output to separate files per (workunit, label)."""
  def __init__(self, run_tracker, formatter, directory):
    Reporter.__init__(self, run_tracker, formatter)
    self._dir = directory
    self._files = {} # path -> file

  def open(self):
    safe_mkdir(os.path.dirname(self._dir))
    Reporter.open(self)

  def close(self):
    Reporter.close(self)
    for f in self._files.values():
      f.close()

  def handle_formatted(self, workunit, label, s):
    if os.path.exists(self._dir):  # Make sure we're not immediately after a clean-all.
      path = self._make_path(workunit, label)
      if path not in self._files:
        f = open(path, 'w')
        self._files[path] = f
      else:
        f = self._files[path]
      f.write(s)
      # We must flush in the same thread as the write.
      f.flush()

  def overwrite_formatted(self, workunit, label, s):
    if os.path.exists(self._dir):  # Make sure we're not immediately after a clean-all.
      with open(self._make_path(workunit, label), 'w') as f:
        f.write(s)

  def _make_path(self, workunit, label):
    if not label or label == WorkUnit.DEFAULT_OUTPUT_LABEL:
      f = 'build.html'
    elif not workunit:
      f = label
    else:
      f = '%s.%s' % (workunit.id, label)
    return os.path.join(self._dir, f)
