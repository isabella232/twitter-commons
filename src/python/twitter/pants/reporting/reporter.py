import os
import sys

from twitter.common.dirutil import safe_mkdir


class Reporter(object):
  def __init__(self, formatter):
    self.formatter = formatter

  def open(self):
    self.handle_formatted_output(self.formatter.header())

  def close(self):
    self.handle_formatted_output(self.formatter.footer())

  def handle_output(self, s):
    self.handle_formatted_output(self.formatter.format(s))

  def handle_formatted_output(self, s):
    raise NotImplementedError('handle_formatted_output() not implemented')

  def enter_scope(self, scopes):
    self.handle_formatted_output(self.formatter.enter_scope(scopes))

  def exit_scope(self, scopes, outcome):
    self.handle_formatted_output(self.formatter.exit_scope(scopes, outcome))


class ConsoleReporter(Reporter):
  def __init__(self, formatter):
    Reporter.__init__(self, formatter)

  def handle_formatted_output(self, s):
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

  def handle_formatted_output(self, s):
    self._file.write(s)
    # Not sure why, but it's important to flush in the same thread as the write, so we must flush here.
    self._file.flush()

