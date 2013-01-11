from twitter.pants.reporting.read_write_buffer import ReadWriteBuffer

class Invocation(object):
  """An invocation of some build tool, typically by spawning a subprocess."""

  def __init__(self, name):
    self.name = name  # A string representing this invocation. May be displayed in reports.
    self.stdout = ReadWriteBuffer()
    self.stderr = ReadWriteBuffer()


