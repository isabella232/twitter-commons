
import cgi
import os
import threading
import time

from twitter.common.dirutil import safe_mkdir
from twitter.common.lang import Compatibility

StringIO = Compatibility.StringIO


def default_reporter(html_report_file):
  report = Report()
  report.add_reporter(ConsoleReporter(PlainTextFormatter()))
  report.add_reporter(FileReporter(HTMLFormatter(), html_report_file))
  return report


class ReportingError(Exception):
  pass


class Formatter(object):
  def format(self, str):
    raise ReportingError('format() not implemented')

  def header(self):
    return ''

  def footer(self):
    return ''


class PlainTextFormatter(Formatter):
  def format(self, str):
    return str


class HTMLFormatter(Formatter):
  def format(self, str):
    return cgi.escape(str).replace('\n', '</br>')

  def header(self):
    return \
"""
<html>
<head>
<title>Report for build</title>
</head>
<body>
"""

  def footer(self):
    return \
"""
</html>
"""


class Reporter(object):
  def __init__(self, formatter):
    self.formatter = formatter

  def open(self):
    self.do_handle_output(self.formatter.header())

  def close(self):
    self.do_handle_output(self.formatter.footer())

  def handle_output(self, str):
    self.do_handle_output(self.formatter.format(str))

  def do_handle_output(self, str):
    raise ReportingError('do_handle_output() not implemented')


class ConsoleReporter(Reporter):
  def __init__(self, formatter):
    Reporter.__init__(self, formatter)

  def do_handle_output(self, str):
    print str


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

  def do_handle_output(self, str):
    self._file.write(str)
    # Not sure why, but it's important to flush in the same thread as the write, so we must flush here.
    self._file.flush()


class Report(object):
  def __init__(self):
    self._timer = threading.Thread(target=self._notify_periodically)
    self._lock = threading.Lock()
    self._shutdown = False
    self._io = StringIO()
    self._period = 0.1
    self._reporters = []
    self._readpos = 0
    self._writepos = 0

  def open(self):
    with self._lock:
      for reporter in self._reporters:
        reporter.open()
    self._timer.start()

  def add_reporter(self, reporter):
    with self._lock:
      self._reporters.append(reporter)

  def write(self, str):
    with self._lock:
      self._io.seek(self._writepos)
      self._io.write(str)
      self._io.flush()
      self._writepos = self._io.tell()

  def close(self):
    with self._lock:
      self._shutdown = True
    self._timer.join()
    with self._lock:
      for reporter in self._reporters:
        reporter.close()

  def _notify_periodically(self):
    while True:
      with self._lock:
        str = self._readline()
        if len(str) > 0:
          self._notify(str)
        if self._shutdown:
          self._io.close()
          return
      time.sleep(self._period)

  def _readline(self):
    self._io.seek(self._readpos)
    ret = self._io.read()
    self._readpos = self._io.tell()
    return ret

  def _notify(self, str):
    for reporter in self._reporters:
      reporter.handle_output(str)
