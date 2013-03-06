import os
import sys

from twitter.common.dirutil import safe_mkdir
from twitter.pants.goal.work_unit import WorkUnit


class Reporter(object):
  def __init__(self, formatter):
    self.formatter = formatter

  def open(self):
    self.handle_formatted_output(None, None, self.formatter.start_run())

  def close(self):
    self.handle_formatted_output(None, None, self.formatter.end_run())

  def start_workunit(self, workunit):
    self.handle_formatted_output(workunit, None, self.formatter.start_workunit(workunit))

  def end_workunit(self, workunit):
    self.handle_formatted_output(workunit, None, self.formatter.end_workunit(workunit))

  def handle_output(self, workunit, label, s):
    """

    label - classifies the output (e.g., 'output' for output pants itself writes directly,
    'stdout'/'stderr' for output captured from a tool's stdout/stderr. Other labels are possible,
    e.g., if we capture output from a tool's logfiles."""
    self.handle_formatted_output(workunit, label, self.formatter.format(workunit, label, s))

  def handle_formatted_output(self, workunit, label, s):
    raise NotImplementedError('handle_formatted_output() not implemented')


class ConsoleReporter(Reporter):
  def __init__(self, formatter):
    Reporter.__init__(self, formatter)

  def handle_formatted_output(self, workunit, label, s):
    sys.stdout.write(s)


class FileReporter(Reporter):
  """Merges all output, for all labels, into one file."""
  def __init__(self, formatter, path):
    Reporter.__init__(self, formatter)
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

  def handle_formatted_output(self, workunit, label, s):
    self._file.write(s)
    # We must flush in the same thread as the write.
    self._file.flush()


class MultiFileReporter(Reporter):
  """Writes all default output to one file, and all other output to separate files per (workunit, label)."""
  def __init__(self, formatter, dir):
    Reporter.__init__(self, formatter)
    self._dir = dir
    self._files = {} # path -> file

  def open(self):
    safe_mkdir(os.path.dirname(self._dir))
    Reporter.open(self)

  def close(self):
    Reporter.close(self)
    for file in self._files.values():
      file.close()

  def handle_formatted_output(self, workunit, label, s):
    if not workunit or not label or label == WorkUnit.DEFAULT_OUTPUT_LABEL:
      path = os.path.join(self._dir, 'build.html')
    else:
      path = os.path.join(self._dir, '%s.%s' % (workunit.id, label))
    if path not in self._files:
      file = open(path, 'w')
      self._files[path] = file
    else:
      file = self._files[path]
    file.write(s)
    # We must flush in the same thread as the write.
    file.flush()

