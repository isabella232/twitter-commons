
class WorkUnit:
  """A hierarchical unit of work, for the purpose of timing and reporting.

  A WorkUnit can be subdivided into further WorkUnits, e.g., there might be one WorkUnit representing an
  entire pants run, and that can be subdivided into WorkUnits for each phase. Each of those can be subdivided
  into WorkUnits for each task, and a task can subdivide that into further work units, if finer-grained
  timing and reporting is needed.
  """

  # The outcome must be one of these values. It can only be set to a new value <= an old one.
  FAILURE = 0
  WARNING = 1
  SUCCESS = 2
  UNKNOWN = 3

  def __init__(self, name, parent=None):
    self._outcome = WorkUnit.UNKNOWN
    self._name = name
    self._parent = parent

  def get_outcome(self):
    return self._outcome

  def set_outcome(self, outcome):
    """Set the outcome of this work unit.

    We can set the outcome on a work unit directly, but that outcome will also be affected by
    those of its subunits. The right thing happens: The outcome of a work unit is the
    worst outcome of any of its subunits and any outcome set on it directly."""
    if outcome < self._outcome:  # Otherwise ignore.
      self._outcome = outcome
      self.choose(0, 0, 0, 0)  # Dummy call, to validate.
      if self._parent: self._parent.set_outcome(self._outcome)

  def get_parent(self):
    return self._parent

  def get_name(self):
    return self._name

  def get_name_hierarchy(self):
    """Returns the names from leaf to root. E.g., ['split', 'scalac', 'compile', 'all']"""
    return [self.get_name()] + ([] if self._parent is None else self._parent.get_name_hierarchy())

  def choose(self, failure_val, warning_val, success_val, unknown_val):
    """Returns one of the 4 arguments, depending on our outcome."""
    if self._outcome not in range(0, 4):
      raise Exception, 'Invalid outcome: %s' % self._outcome
    return (failure_val, warning_val, success_val, unknown_val)[self._outcome]

  def outcome_string(self):
    return self.choose('FAILURE', 'WARNING', 'SUCCESS', 'UNKNOWN')
