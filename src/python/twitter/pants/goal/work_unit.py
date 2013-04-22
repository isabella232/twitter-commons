import os
import re
import time
import uuid

from twitter.common.dirutil import safe_mkdir_for
from twitter.pants.goal.read_write_buffer import FileBackedRWBuf


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

  # The types must be an iterable of these values.
  SETUP = 0
  SETUP = 0
  PHASE = 1
  GOAL = 2
  GROUP = 3
  TOOL = 4
  MULTITOOL = 5
  TEST = 6
  JVM = 7
  NAILGUN = 8
  REPL = 9

  def __init__(self, run_tracker, parent, name, types=(), cmd=''):
    """
    - run_tracker: The RunTracker that tracks this WorkUnit.
    - parent: The containing workunit, if any. E.g., 'compile' might contain 'java', 'scala' etc.,
              'scala' might contain 'compile', 'split' etc.
    - name: A short name for this work. E.g., 'resolve', 'compile', 'scala', 'zinc'.
    - types: An optional iterable of types. The reporters can use this to decide how to
             display information about this work.
    - cmd: An optional longer string representing this work.
           E.g., the cmd line of a compiler invocation.
    """
    self._outcome = WorkUnit.UNKNOWN

    self.run_tracker = run_tracker
    self.parent = parent
    self.children = []
    self.name = name
    self.types = set(types)
    self.cmd = cmd
    self.id = uuid.uuid4()
    # In seconds since the epoch. Doubles, to account for fractional seconds.
    self.start_time = 0
    self.end_time = 0

    # A workunit may have multiple outputs, which we identify by a label.
    # E.g., a tool invocation may have 'stdout', 'stderr', 'debug_log' etc.
    self._outputs = {}  # label -> output buffer.

    if self.parent:
      self.parent.children.append(self)

  def is_tool(self):
    return WorkUnit.TOOL in self.types

  def is_multitool(self):
    return WorkUnit.MULTITOOL in self.types

  def is_test(self):
    return WorkUnit.TEST in self.types

  def start(self):
    self.start_time = time.time()

  def end(self):
    self.end_time = time.time()
    for output in self._outputs.values():
      output.close()
    self.run_tracker.cumulative_timings.add_timing(self.get_path(), self.duration(), self.is_tool())
    self.run_tracker.self_timings.add_timing(self.get_path(), self._self_time(), self.is_tool())

  def to_dict(self):
    """Useful for providing arguments to templates."""
    ret = {}
    for key in ['name', 'cmd', 'id', 'start_time', 'end_time',
                'outcome', 'start_time_string', 'start_delta_string']:
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

  _valid_label_re = re.compile(r'\w+')
  def output(self, label):
    m = WorkUnit._valid_label_re.match(label)
    if not m or m.group(0) != label:
      raise Exception('Invalid label: %s' % label)
    if label not in self._outputs:
      path = os.path.join(self.run_tracker.info_dir, 'tool_outputs', '%s.%s' % (self.id, label))
      safe_mkdir_for(path)
      self._outputs[label] = FileBackedRWBuf(path)
    return self._outputs[label]

  def outputs(self):
    return self._outputs

  def choose(self, aborted_val, failure_val, warning_val, success_val, unknown_val):
    """Returns one of the 5 arguments, depending on our outcome."""
    if self._outcome not in range(0, 5):
      raise Exception('Invalid outcome: %s' % self._outcome)
    return (aborted_val, failure_val, warning_val, success_val, unknown_val)[self._outcome]

  def outcome_string(self):
    return self.choose('ABORTED', 'FAILURE', 'WARNING', 'SUCCESS', 'UNKNOWN')

  def duration(self):
    """Returns the time (in fractional seconds) spent in this workunit and its children."""
    return (self.end_time or time.time()) - self.start_time

  def start_delta(self):
    """How long (in whole seconds) after this run started did this workunit start."""
    return int(self.start_time) - int(self.run_tracker.root_workunit.start_time)

  def start_time_string(self):
    """A convenient string representation of start_time."""
    return time.strftime('%H:%M:%S', time.localtime(self.start_time))

  def start_delta_string(self):
    """A conveneint string representation of start_delta()."""
    delta = self.start_delta()
    return '%02d:%02d' % (delta / 60, delta % 60)

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
    """Returns non-leaf time spent in this workunit."""
    return 0 if len(self.children) == 0 else self._self_time()

  def _self_time(self):
    """Returns the time spent in this workunit outside of any children."""
    return self.duration() - sum([child.duration() for child in self.children])

