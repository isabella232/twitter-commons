import os
import sys

from twitter.common.dirutil import safe_mkdir


class Reporter(object):
  def __init__(self, formatter):
    self.formatter = formatter

  def open(self):
    self.handle_formatted_output(None, self.formatter.header())

  def close(self):
    self.handle_formatted_output(None, self.formatter.footer())

  def handle_output(self, workunit, s):
    self.handle_formatted_output(workunit, self.formatter.format(s))

  def handle_formatted_output(self, workunit, s):
    raise NotImplementedError('handle_formatted_output() not implemented')

  def start_workunit(self, workunit):
    self.handle_formatted_output(self.formatter.start_workunit(workunit))

  def end_workunit(self, workunit):
    self.handle_formatted_output(self.formatter.end_workunit(workunit))


class ConsoleReporter(Reporter):
  def __init__(self, formatter):
    Reporter.__init__(self, formatter)

  def handle_formatted_output(self, workunit, s):
    sys.stdout.write(s)


class FileReporter(Reporter):
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

  def handle_formatted_output(self, workunit, s):
    self._file.write(s)
    # Not sure why, but it's important to flush in the same thread as the write, so we must flush here.
    self._file.flush()

