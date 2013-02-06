
import uuid
from collections import defaultdict

from twitter.pants.goal.read_write_buffer import ReadWriteBuffer

class WorkUnit(object):
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

    self.parent = parent
    self.type = type
    self.name = name
    self.cmd = cmd
    self.id = uuid.uuid4()
    # In seconds since the epoch. Doubles, to account for fractional seconds.
    self.start_time = 0
    self.end_time = 0

    # A workunit may have multiple outputs, which we identify by a label.
    # E.g., a tool invocation may have 'stdout', 'stderr', 'debug_log' etc.
    self._outputs = defaultdict(ReadWriteBuffer)  # label -> output buffer.

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
      if self.parent: self.parent.set_outcome(self._outcome)

  DEFAULT_OUTPUT_LABEL = 'default'
  def output(self, label=DEFAULT_OUTPUT_LABEL):
    return self._outputs[label]

  def outputs(self):
    return self._outputs

  def choose(self, failure_val, warning_val, success_val, unknown_val):
    """Returns one of the 4 arguments, depending on our outcome."""
    if self._outcome not in range(0, 4):
      raise Exception, 'Invalid outcome: %s' % self._outcome
    return (failure_val, warning_val, success_val, unknown_val)[self._outcome]

  def outcome_string(self):
    return self.choose('FAILURE', 'WARNING', 'SUCCESS', 'UNKNOWN')


