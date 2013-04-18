import threading

from twitter.common.lang import Compatibility

StringIO = Compatibility.StringIO


class InMemoryRWBuf(object):
  """An unbounded read-write buffer entirely in memory.

  Can be used as a file-like object for reading and writing. Note that it can't be used in
  situations that require a real file (e.g., redirecting stdout/stderr of subprocess.Popen())."""
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


class FileBackedRWBuf(object):
  """An unbounded read-write buffer backed by a file.

  Can be used as a file-like object for reading and writing the underlying file. Has a fileno,
  so you can redirect stdout/stderr of subprocess.Popen() etc. to this object. This is useful
  when you want to poll the output of long-running subprocesses in a separate thread."""
  def __init__(self, backing_file):
    self._lock = threading.Lock()
    self._backing_file = backing_file
    self._out = open(backing_file, 'a')
    self._in = open(backing_file, 'r')
    self.fileno = self._out.fileno
    self._readpos = 0

  def write(self, s):
    with self._lock:
      self._out.write(str(s))
      self._out.flush()

  def read(self, size=-1):
    with self._lock:
      self._in.seek(self._readpos)
      ret = self._in.read() if size == -1 else self._in.read(size)
      self._readpos = self._in.tell()
      return ret

  def flush(self):
    self._out.flush()

  def close(self):
    self._out.close()
    self._in.close()
