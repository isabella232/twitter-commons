import threading

from twitter.common.lang import Compatibility

StringIO = Compatibility.StringIO


class ReadWriteBuffer(object):
  """An unbounded read-write buffer.

  Can be used as a file-like object, e.g., you can redirect stdout/stderr to it when spawning subprocesses.
  This useful when you want to poll the output of long-running subprocesses in a separate thread."""
  def __init__(self):
    self._lock = threading.Lock()
    self._io = StringIO()
    self._readpos = 0
    self._writepos = 0

  def write(self, s):
    with self._lock:
      self._io.seek(self._writepos)
      self._io.write(str(s))
      self._io.flush()
      self._writepos = self._io.tell()

  def read(self, size=-1):
    with self._lock:
      self._io.seek(self._readpos)
      ret = self._io.read() if size == -1 else self._io.read(size)
      self._readpos = self._io.tell()
      return ret

  def flush(self):
    pass

