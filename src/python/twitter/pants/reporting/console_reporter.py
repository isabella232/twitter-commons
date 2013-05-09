import sys

from collections import defaultdict

from twitter.pants.goal.workunit import WorkUnit
from twitter.pants.reporting.report import Reporter


class ConsoleReporter(Reporter):
  """Plain-text reporting to stdout."""

  def __init__(self, run_tracker, indenting):
    Reporter.__init__(self, run_tracker)
    self._indenting = indenting
    # We don't want spurious newlines between nested workunits, so we only emit them
    # when we need to write content to the workunit.
    # TODO: protect self._needs_newline against concurrent access.
    self._needs_newline = defaultdict(bool)  # workunit id -> bool.

  def open(self):
    pass

  def close(self):
    if self.run_tracker.options.time:
      print('\n')
      print('Cumulative Timings')
      print('==================')
      print(self._format_aggregated_timings(self.run_tracker.cumulative_timings))
      print('\n')
      print('Self Timings')
      print('============')
      print(self._format_aggregated_timings(self.run_tracker.self_timings))
      print('\n')
      print('Artifact Cache Stats')
      print('====================')
      print(self._format_artifact_cache_stats(self.run_tracker.artifact_cache_stats))
    print('')

  def start_workunit(self, workunit):
    if workunit.parent and workunit.parent.has_label(WorkUnit.MULTITOOL):
      sys.stdout.write('.')
    else:
      sys.stdout.write('\n%s %s %s[%s]' %
                       (workunit.start_time_string(),
                        workunit.start_delta_string(),
                        self._indent(workunit),
                        workunit.name if self._indenting else workunit.get_path()))
    sys.stdout.flush()

  def end_workunit(self, workunit):
    if workunit.parent:
      self._needs_newline[workunit.parent.id] = False

  def handle_output(self, workunit, label, s):
    # Emit output from test frameworks, but not from other tools.
    if workunit.workunit.has_label(WorkUnit.TEST):
      if not self._needs_newline[workunit.id]:
        s = '\n' + s
        self._needs_newline[workunit.id] = True
      sys.stdout.write(self._prefix(workunit, s))
      sys.stdout.flush()

  def handle_message(self, workunit, *msg_elements):
    elements = [e if isinstance(e, basestring) else e[0] for e in msg_elements]
    if not self._needs_newline[workunit.id]:
      elements.insert(0, '\n')
      self._needs_newline[workunit.id] = True
    sys.stdout.write(self._prefix(workunit, ''.join(elements)))

  def _format_aggregated_timings(self, aggregated_timings):
    return '\n'.join(['%(timing).3f %(label)s' % x for x in aggregated_timings.get_all()])

  def _format_artifact_cache_stats(self, artifact_cache_stats):
    stats = artifact_cache_stats.get_all()
    return 'No artifact cache reads.' if not stats else \
    '\n'.join(['%(cache_name)s - Hits: %(num_hits)d Misses: %(num_misses)d' % x
               for x in stats])

  def _indent(self, workunit):
    return '  ' * (len(workunit.ancestors()) - 1)

  _time_string_filler = ' ' * 15
  def _prefix(self, workunit, s):
    if self._indenting:
      return s.replace('\n', '\n' + ConsoleReporter._time_string_filler + self._indent(workunit))
    else:
      return ConsoleReporter._time_string_filler + s

