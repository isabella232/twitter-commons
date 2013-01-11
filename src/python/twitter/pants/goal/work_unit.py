import uuid
from twitter.pants.goal.read_write_buffer import ReadWriteBuffer

class WorkUnit:
  """A hierarchical unit of work, for the purpose of timing and reporting.

  A WorkUnit can be subdivided into further WorkUnits. The WorkUnit concept is deliberately decoupled from the
  phase/task hierarchy, although it will typically be used to represent it in reports. This allows some
  flexibility in having, say, sub-units inside a task. E.g., there might be one WorkUnit representing an
  entire pants run, and that can be subdivided into WorkUnits for each phase. Each of those can be subdivided
  into WorkUnits for each task, and a task can subdivide that into further work units, if finer-grained
  timing and reporting is needed.
  """

  # The outcome must be one of these values. It can only be set to a new value <= an old one.
  FAILURE = 0
  WARNING = 1
  SUCCESS = 2
  UNKNOWN = 3

  def __init__(self, parent, type, name, cmd):
    """
    - parent: The containing workunit, if any. E.g., 'compile' might contain 'java', 'scala' etc., and
              'scala' might contain 'compile', 'split' etc.
    - type: A string that the report formatters can use to decide how to display information
            about this work. E.g., 'phase', 'goal', 'tool'.
    - name: A short name for this work. E.g., 'resolve', 'compile', 'scala', 'zinc'.
    - cmd: An optional longer string representing this work. E.g., the cmd line of a
           compiler invocation. Used only for display.
    """
    self._outcome = WorkUnit.UNKNOWN
    self._parent = parent
    self._type = type
    self._name = name
    self._cmd = cmd
    self._id = uuid.uuid4()
    self._stdout = ReadWriteBuffer()  # Output for this work unit (but not its children) goes here.
    self._stderr = ReadWriteBuffer()  # Do we need this? Let's see.

  def get_outcome(self):
    return self._outcome

  def set_outcome(self, outcome):
    """Set the outcome of this work unit.

    We can set the outcome on a work unit directly, but that outcome will also be affected by
    those of its subunits. The right thing happens: The outcome of a work unit is the
    worst outcome of any of its subunits and any outcome set on it directly."""
    if outcome < self._outcome:
      self._outcome = outcome
      self.choose(0, 0, 0, 0)  # Dummy call, to validate.
      if self._parent: self._parent.set_outcome(self._outcome)

  def parent(self):
    """The enclosing workunit, or None of this is the root workunit."""
    return self._parent

  def type(self):
    """The type of this workunit.

    A string that a reporter can use to decide how to format output for this workunit."""
    return self._type

  def name(self):
    """The short name of this workunit."""
    return self._name

  def cmd(self):
    """A long string representing the work done by this workunit, e.g., a cmd-line."""
    return self._cmd

  def id(self):
    """The unique id of this workunit."""
    return self._id

  def stdout(self):
    """Write output from execution of this workunit here."""
    return self._stdout

  def stderr(self):
    """Write errors from execution of this workunit here."""
    return self._stderr

  def choose(self, failure_val, warning_val, success_val, unknown_val):
    """Returns one of the 4 arguments, depending on our outcome."""
    if self._outcome not in range(0, 4):
      raise Exception, 'Invalid outcome: %s' % self._outcome
    return (failure_val, warning_val, success_val, unknown_val)[self._outcome]

  def outcome_string(self):
    return self.choose('FAILURE', 'WARNING', 'SUCCESS', 'UNKNOWN')
