
import time
import uuid

from collections import defaultdict


from twitter.pants.goal.read_write_buffer import ReadWriteBuffer

class WorkUnit(object):
  """A hierarchical unit of work, for the purpose of timing and reporting.

  A WorkUnit can be subdivided into further WorkUnits. The WorkUnit concept is deliberately
  decoupled from the phase/task hierarchy, although it will typically be used to represent it in
  reports. This allows some flexibility in having, say, sub-units inside a task. E.g., there might
  be one WorkUnit representing an entire pants run, and that can be subdivided into WorkUnits for
  each phase. Each of those can be subdivided into WorkUnits for each task, and a task can
  subdivide that into further work units, if finer-grained timing and reporting is needed.
  """

  # The outcome must be one of these values. It can only be set to a new value <= an old one.
  ABORTED = 0
  FAILURE = 1
  WARNING = 2
  SUCCESS = 3
  UNKNOWN = 4

  def __init__(self, parent, aggregated_timings, name, type, cmd):
    """
    - parent: The containing workunit, if any. E.g., 'compile' might contain 'java', 'scala' etc.,
              'scala' might contain 'compile', 'split' etc.
    - name: A short name for this work. E.g., 'resolve', 'compile', 'scala', 'zinc'.
    - type: An optional string that the report formatters can use to decide how to display
            information about this work. E.g., 'phase', 'goal', 'jvm_tool'. By convention, types
            ending with '_tool' are assumed to be invocations of external tools.
    - cmd: An optional longer string representing this work. E.g., the cmd line of a
           compiler invocation. Used only for display.
    """
    self._outcome = WorkUnit.UNKNOWN

    self.parent = parent
    self.aggregated_timings = aggregated_timings
    self.children = []
    self.name = name
    self.type = type
    self.cmd = cmd
    self.id = uuid.uuid4()
    # In seconds since the epoch. Doubles, to account for fractional seconds.
    self.start_time = 0
    self.end_time = 0

    # A workunit may have multiple outputs, which we identify by a label.
    # E.g., a tool invocation may have 'stdout', 'stderr', 'debug_log' etc.
    self._outputs = defaultdict(ReadWriteBuffer)  # label -> output buffer.

    if self.parent:
      self.parent.children.append(self)

  def is_tool(self):
    return self.type.endswith('_tool')

  def is_multitool(self):
    return self.type.endswith('_multitool')

  def start(self):
    self.start_time = time.time()

  def end(self):
    self.end_time = time.time()
    self.aggregated_timings.add_timing(self.get_path(), self.self_time(), self.is_tool())

  def to_dict(self):
    """Useful for providing arguments to templates."""
    ret = {}
    for key in ['type', 'name', 'cmd', 'id', 'start_time', 'end_time',
                'outcome', 'start_time_string', 'end_time_string']:
      val = getattr(self, key)
      ret[key] = val() if hasattr(val, '__call__') else val
    ret['parent'] = self.parent.to_dict() if self.parent else None
    return ret

  def outcome(self):
    return self._outcome

  def set_outcome(self, outcome):
    """Set the outcome of this work unit.

    We can set the outcome on a work unit directly, but that outcome will also be affected by
    those of its subunits. The right thing happens: The outcome of a work unit is the
    worst outcome of any of its subunits and any outcome set on it directly."""
    if outcome < self._outcome:
      self._outcome = outcome
      self.choose(0, 0, 0, 0, 0)  # Dummy call, to validate.
      if self.parent: self.parent.set_outcome(self._outcome)

  DEFAULT_OUTPUT_LABEL = 'build'
  def output(self, label=DEFAULT_OUTPUT_LABEL):
    return self._outputs[label]

  def outputs(self):
    return self._outputs

  def choose(self, aborted_val, failure_val, warning_val, success_val, unknown_val):
    """Returns one of the 5 arguments, depending on our outcome."""
    if self._outcome not in range(0, 5):
      raise Exception, 'Invalid outcome: %s' % self._outcome
    return (aborted_val, failure_val, warning_val, success_val, unknown_val)[self._outcome]

  def outcome_string(self):
    return self.choose('ABORTED', 'FAILURE', 'WARNING', 'SUCCESS', 'UNKNOWN')

  def duration(self):
    """Returns the time spent in this workunit and its children."""
    return self.end_time - self.start_time

  def self_time(self):
    """Returns the time spent in this workunit outside of any children."""
    return self.duration() - self.time_in_children()

  def time_in_children(self):
    return sum([child.duration() for child in self.children])

  def start_time_string(self):
    return self._format_time_string(self.start_time)

  def end_time_string(self):
    return self._format_time_string(self.end_time)

  def _format_time_string(self, secs):
    return time.strftime('%H:%M:%S', time.localtime(secs))

  def ancestors(self):
    """Returns a list of this workunit and those enclosing it, up to the root."""
    ret = []
    workunit = self
    while workunit is not None:
      ret.append(workunit)
      workunit = workunit.parent
    return ret

  def get_path(self):
    """Returns a path string for this workunit, E.g., 'all:compile:jvm:scalac'."""
    return ':'.join(reversed([w.name for w in self.ancestors()]))

  def unaccounted_time(self):
    """Returns the difference between the time spent in our children and own time."""
    return 0 if len(self.children) == 0 else self.self_time()

