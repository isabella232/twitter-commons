
from __future__ import print_function

import os
import re
import sys
import time

from collections import defaultdict
from contextlib import contextmanager


from twitter.common.collections import OrderedSet
from twitter.common.dirutil import Lock, safe_mkdir_for
from twitter.common.process import ProcessProviderFactory

from twitter.pants import get_buildroot
from twitter.pants import SourceRoot
from twitter.pants.base import ParseContext
from twitter.pants.reporting import default_reporting
from twitter.pants.base.target import Target
from twitter.pants.targets import Pants
from twitter.pants.goal.products import Products


# Utility definition for grabbing process info for locking.
def _process_info(pid):
  ps = ProcessProviderFactory.get()
  ps.collect_set([pid])
  handle = ps.get_handle(pid)
  cmdline = handle.cmdline().replace('\0', ' ')
  return '%d (%s)' % (pid, cmdline)


class RunInfo(object):
  """A little plaintext file containing very basic info about a pants run.

  Can only be appended to, never edited."""
  def __init__(self, info_file):
    self._info_file = info_file
    safe_mkdir_for(self._info_file)
    self._info = {}
    if os.path.exists(self._info_file):
      with open(self._info_file, 'r') as infile:
        info = infile.read()
      for m in re.finditer("""^([^:]+):(.*)$""", info, re.MULTILINE):
        self._info[m.group(1).strip()] = m.group(2).strip()

  def get_info(self, key):
    return self._info.get(key, None)

  def __getattr__(self, key):
    ret = self.get_info(key)
    if ret is None:
      raise KeyError, key
    return ret

  def get_as_dict(self):
    return self._info.copy()

  def add_info(self, key, val):
    self.add_infos([(key, val)])

  def add_infos(self, keyvals):
    with open(self._info_file, 'a') as outfile:
      for key, val in keyvals:
        if ':' in key:
          raise Exception, 'info key must not contain a colon'
        outfile.write('%s: %s\n' % (key, val))
        self._info[key] = val


class Context(object):
  """Contains the context for a single run of pants.

  Goal implementations can access configuration data from pants.ini and any flags they have exposed
  here as well as information about the targets involved in the run.

  Advanced uses of the context include adding new targets to it for upstream or downstream goals to
  operate on and mapping of products a goal creates to the targets the products are associated with.
  """

  class Log(object):
    def debug(self, msg): pass
    def info(self, msg): pass
    def warn(self, msg): pass

  class Outcome:
    # status must be one of these values. Status can only be set to a new value <= an old one.
    FAILURE = 0
    WARNING = 1
    SUCCESS = 2
    UNKNOWN = 3

    @staticmethod
    def _choose(status, failure_val, warning_val, success_val, unknown_val):
      if status not in range(0, 4):
        raise Exception, 'Invalid status: %s' % status
      return (failure_val, warning_val, success_val, unknown_val)[status]

    @staticmethod
    def _str(status):
      return Context.Outcome._choose(status, 'FAILURE', 'WARNING', 'SUCCESS', 'UNKNOWN')

    def __init__(self):
      self._status = Context.Outcome.UNKNOWN

    def choose(self, failure_val, warning_val, success_val, unknown_val=None):
      """Returns one of the 4 arguments, depending on our status."""
      return Context.Outcome._choose(self._status, failure_val, warning_val, success_val, unknown_val)

    def __str__(self):
      return Context.Outcome._str(self._status)

    def get_status(self):
      return self._status

    def set_status(self, status):
      if status < self._status:  # Otherwise ignore.
        self._status = status
        self.choose(0, 0, 0, 0)  # Dummy call, to validate status.


  def __init__(self, config, options, target_roots, lock=Lock.unlocked(), log=None):
    run_timestamp = time.time()
    # run_id is safe for use in paths.
    millis = (run_timestamp * 1000) % 1000
    run_id = 'pants_run_%s_%d' % (time.strftime('%Y_%m_%d_%H_%M_%S', time.localtime(run_timestamp)), millis)
    cmd_line = ' '.join(['pants'] + sys.argv[1:])
    self._run_info = RunInfo(os.path.join(config.getdefault('info_dir'), '%s.info' % run_id))
    self._run_info.add_infos([('id', run_id), ('timestamp', run_timestamp), ('cmd_line', cmd_line)])
    self._outcome = Context.Outcome()

    self._config = config
    self._options = options
    self._lock = lock
    self._log = log or Context.Log()
    self._state = {}
    self._products = Products()
    self._buildroot = get_buildroot()

    self.replace_targets(target_roots)
    self.reporter = default_reporting(self)
    self.reporter.open()

  @property
  def run_info(self):
    """Returns the info for this pants run."""
    return self._run_info

  @property
  def outcome(self):
    """Returns the outcome of this pants run."""
    return self._outcome

  @property
  def config(self):
    """Returns a Config object containing the configuration data found in pants.ini."""
    return self._config

  @property
  def options(self):
    """Returns the command line options parsed at startup."""
    return self._options

  @property
  def lock(self):
    """Returns the global pants run lock so a goal can release it if needed."""
    return self._lock

  @property
  def log(self):
    """Returns the preferred logger for goals to use."""
    return self._log

  @property
  def products(self):
    """Returns the Products manager for the current run."""
    return self._products

  @property
  def target_roots(self):
    """Returns the targets specified on the command line.

    This set is strictly a subset of all targets in play for the run as returned by self.targets().
    Note that for a command line invocation that uses wildcard selectors : or ::, the targets
    globbed by the wildcards are considered to be target roots.
    """
    return self._target_roots

  def __str__(self):
    return 'Context(id:%s, state:%s, targets:%s)' % (self.id, self.state, self.targets())

  def acquire_lock(self):
    """ Acquire the global lock for the root directory associated with this context. When
    a goal requires serialization, it will call this to acquire the lock.
    """
    def onwait(pid):
      print('Waiting on pants process %s to complete' % _process_info(pid), file=sys.stderr)
      return True
    if self._lock.is_unlocked():
      runfile = os.path.join(self._buildroot, '.pants.run')
      self._lock = Lock.acquire(runfile, onwait=onwait)

  def release_lock(self):
    """Release the global lock if it's held.
    Returns True if the lock was held before this call.
    """
    if self._lock.is_unlocked():
      return False
    else:
      self._lock.release()
      self._lock = Lock.unlocked()
      return True

  def is_unlocked(self):
    """Whether the global lock object is actively holding the lock."""
    return self._lock.is_unlocked()

  def replace_targets(self, target_roots):
    """Replaces all targets in the context with the given roots and their transitive
    dependencies.
    """
    self._target_roots = target_roots
    self._targets = OrderedSet()
    for target in target_roots:
      self.add_target(target)
    self.id = Target.identify(self._targets)

  def add_target(self, target):
    """Adds a target and its transitive dependencies to the run context.

    The target is not added to the target roots.
    """
    def add_targets(tgt):
      self._targets.update(tgt.resolve())
    target.walk(add_targets)

  def add_new_target(self, target_base, target_type, *args, **kwargs):
    """Creates a new target, adds it to the context and returns it.

    This method ensures the target resolves files against the given target_base, creating the
    directory if needed and registering a source root.
    """
    if 'derived_from' in kwargs:
      derived_from = kwargs.get('derived_from')
      del kwargs['derived_from']
    else:
      derived_from = None
    target = self._create_new_target(target_base, target_type, *args, **kwargs)
    self.add_target(target)
    if derived_from:
      target.derived_from = derived_from
    return target

  def _create_new_target(self, target_base, target_type, *args, **kwargs):
    if not os.path.exists(target_base):
      os.makedirs(target_base)
    SourceRoot.register(target_base, target_type)
    with ParseContext.temp(target_base):
      return target_type(*args, **kwargs)

  def remove_target(self, target):
    """Removes the given Target object from the context completely if present."""
    if target in self.target_roots:
      self.target_roots.remove(target)
    self._targets.discard(target)

  def targets(self, predicate=None):
    """Selects targets in-play in this run from the target roots and their transitive dependencies.

    If specified, the predicate will be used to narrow the scope of targets returned.
    """
    return filter(predicate, self._targets)

  def dependants(self, on_predicate=None, from_predicate=None):
    """Returns  a map from targets that satisfy the from_predicate to targets they depend on that
      satisfy the on_predicate.
    """
    core = set(self.targets(on_predicate))
    dependees = defaultdict(set)
    for target in self.targets(from_predicate):
      if hasattr(target, 'dependencies'):
        for dependency in target.dependencies:
          if dependency in core:
            dependees[target].add(dependency)
    return dependees

  def resolve(self, spec):
    """Returns an iterator over the target(s) the given address points to."""
    with ParseContext.temp():
      return Pants(spec).resolve()

  def report(self, str):
    self.reporter.write(str)

  @contextmanager
  def state(self, key, default=None):
    value = self._state.get(key, default)
    yield value
    self._state[key] = value
