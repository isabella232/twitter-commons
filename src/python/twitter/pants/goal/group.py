from collections import defaultdict

from twitter.common.collections import OrderedDict, OrderedSet
from twitter.pants import is_internal
from twitter.pants.targets import InternalTarget
from twitter.pants.tasks import TaskError

class Group(object):
  @staticmethod
  def execute(phase, tasks_by_goal, context, executed):
    """Executes the named phase against the current context tracking goal executions in executed."""

    def execute_task(name, task, targets):
      """Execute and time a single goal that has had all of its dependencies satisfied."""
      start = context.timer.now() if context.timer else None
      try:
        # TODO (Senthil Kumaran):
        # Possible refactoring of the Task Execution Logic (AWESOME-1019)
        if getattr(context.options, 'explain', None):
          context.log.debug("Skipping execution of %s in explain mode" % name)
        else:
          task.execute(targets)
      finally:
        elapsed = context.timer.now() - start if context.timer else None
        if phase not in executed:
          executed[phase] = OrderedDict()
        if elapsed:
          phase_timings = executed[phase]
          if name not in phase_timings:
            phase_timings[name] = []
          phase_timings[name].append(elapsed)

    tasks_by_goalname = dict((goal.name, task.__class__.__name__)
                             for goal, task in tasks_by_goal.items())

    def expand_goal(goal):
      if len(goal) == 2: # goal is (group, goal)
        group_name, goal_name = goal
        task_name = tasks_by_goalname[goal_name]
        return "%s:%s->%s" % (group_name, goal_name, task_name)
      else:
        task_name = tasks_by_goalname[goal]
        return "%s->%s" % (goal, task_name)

    if phase not in executed:
      # Note the locking strategy: We lock the first time we need to, and hold the lock until we're
      # done, even if some of our deps don't themselves need to be serialized. This is because we
      # may implicitly rely on pristine state from an earlier phase.
      locked_by_me = False

      if context.is_unlocked() and phase.serialize():
        context.acquire_lock()
        locked_by_me = True
      # Satisfy dependencies first
      goals = phase.goals()
      if not goals:
        raise TaskError('No goals installed for phase %s' % phase)

      for goal in goals:
        for dependency in goal.dependencies:
          Group.execute(dependency, tasks_by_goal, context, executed)

      runqueue = []
      goals_by_group = {}
      for goal in goals:
        if goal.group:
          group_name = goal.group.name
          if group_name not in goals_by_group:
            group_goals = [goal]
            runqueue.append((group_name, group_goals))
            goals_by_group[group_name] = group_goals
          else:
            goals_by_group[group_name].append(goal)
        else:
          runqueue.append((None, [goal]))

      # OrderedSet takes care of not repeating chunked task execution mentions
      execution_phases = defaultdict(OrderedSet)

      for group_name, goals in runqueue:
        if not group_name:
          goal = goals[0]
          context.log.info('[%s:%s]' % (phase, goal.name))
          execution_phases[phase].add(goal.name)
          execute_task(goal.name, tasks_by_goal[goal], context.targets())
        else:
          for chunk in Group._create_chunks(context, goals):
            for goal in goals:
              goal_chunk = filter(goal.group.predicate, chunk)
              if len(goal_chunk) > 0:
                context.log.info('[%s:%s:%s]' % (phase, group_name, goal.name))
                execution_phases[phase].add((group_name, goal.name))
                execute_task(goal.name, tasks_by_goal[goal], goal_chunk)

      if getattr(context.options, 'explain', None):
        for phase, goals in execution_phases.items():
          goal_to_task = ", ".join(expand_goal(goal) for goal in goals)
          print("%s [%s]" % (phase, goal_to_task))

      # Can't put this in a finally block because some tasks fork, and the forked processes would
      # execute this block as well.
      if locked_by_me:
        context.release_lock()

  @staticmethod
  def compute_exclusives_chunks(targets):
    """ Compute the set of distinct chunks are required based on exclusives.
    If two targets have different values for a particular exclusives tag,
    then those targets must end up in different partitions.
    This method computes the exclusives values that define each chunk.
    e.g.: if target a has exclusives {"x": "1", "z": "1"}, target b has {"x": "2"},
    target c has {"y", "1"}, and target d has {"y", "2", "z": "1"}, then we need to
    perform chunk partitioning on exclusives tags "x" and "y". We don't need to include
    "z" in the chunk partition specification, because there are no conflicts on z.

    Parameters:
      targets: a list of the targets being built.
    Return: the set of exclusives tags that should be used for chunking.
    """
    exclusives_map = defaultdict(set)
    for t in targets:
      if t.exclusives is not None:
        for k in t.exclusives:
          exclusives_map[k] |= t.exclusives[k]
    conflicting_keys = []
    for k in exclusives_map:
      if len(exclusives_map[k]) > 1:
        conflicting_keys.append(k)
    chunks = defaultdict(list)
    for t in targets:
      # compute an exclusives group key: a list of the exclusives values for the keys
      # in the conflicting keys list.
      target_key = []
      for k in conflicting_keys:
        if len(t.exclusives[k]) > 0:
          target_key.append(list(t.exclusives[k])[0])
        else:
          target_key.append("none")
      chunks[str(target_key)].append(t)
    return chunks

  @staticmethod
  def _create_chunks(context, goals):

    def discriminator(target):
      for i, goal in enumerate(goals):
        if goal.group.predicate(target):
          return i
      return 'other'

    # First, divide the set of all targets to be built into compatible chunks, based
    # on their declared exclusives. Then, for each chunk of compatible exclusives, do
    # further subchunking. At the end, we'll have a list of chunks to be built,
    # which will go through the chunks of each exclusives-compatible group separately.

    # TODO(markcc); chunks with incompatible exclusives require separate ivy resolves.
    # Either interleave the ivy task in this group so that it runs once for each batch of
    # chunks with compatible exclusives, or make the compilation tasks do their own ivy resolves
    # for each batch of targets they're asked to compile.
    excl_chunks = Group.compute_exclusives_chunks(context.targets())

    all_chunks = []

    for excl_chunk_key in excl_chunks:

      # TODO(John Sirois): coalescing should be made available in another spot, InternalTarget is jvm
      # specific, and all we care is that the Targets have dependencies defined

      chunk_targets = excl_chunks[excl_chunk_key]
      # need to extract the targets for this chunk that are internal.
      coalesced = InternalTarget.coalesce_targets(context.targets(is_internal), discriminator)
      coalesced = list(reversed(coalesced))

      def not_internal(target):
        return not is_internal(target)
      # got targets that aren't internal.
      rest = OrderedSet(context.targets(not_internal))


      chunks = [rest] if rest else []
      flavor = None
      chunk_start = 0
      for i, target in enumerate(coalesced):
        target_flavor = discriminator(target)
        if target_flavor != flavor and i > chunk_start:
          chunks.append(OrderedSet(coalesced[chunk_start:i]))
          chunk_start = i
        flavor = target_flavor
      if chunk_start < len(coalesced):
        chunks.append(OrderedSet(coalesced[chunk_start:]))
      all_chunks += chunks

    context.log.debug('::: created chunks(%d)' % len(all_chunks))
    for i, chunk in enumerate(all_chunks):
      context.log.debug('  chunk(%d):\n\t%s' % (i, '\n\t'.join(sorted(map(str, chunk)))))

    return all_chunks

  def __init__(self, name, predicate):
    self.name = name
    self.predicate = predicate

  def __repr__(self):
    return "Group(%s,%s)" % (self.name, self.predicate.__name__)
