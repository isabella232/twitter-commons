import sys

from twitter.pants.goal.work_unit import WorkUnit
from twitter.pants.reporting.reporter import Reporter


class ConsoleReporter(Reporter):
  def __init__(self, run_tracker, indenting):
    Reporter.__init__(self, run_tracker)
    self._indenting = indenting

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
      print(self._format_artifact_cache_stats(self.run_tracker.self_timings))
      print('\n')
      print('Artifact Cache Stats')
      print('====================')
      print(self._format_artifact_cache_stats(self.run_tracker.artifact_cache_stats))
    print('')
    Reporter.close(self)

  def start_workunit(self, workunit):
    if workunit.parent and workunit.parent.is_multitool():
      sys.stdout.write('.')
    else:
      sys.stdout.write(self._prefix(workunit,
        '[%s]' % workunit.name if self._indenting else workunit.get_path(),
        with_time_string=True))

  def end_workunit(self, workunit):
    pass

  def handle_output(self, workunit, label, s):
    if label == WorkUnit.DEFAULT_OUTPUT_LABEL or label is None or workunit.type.startswith('test_'):
      sys.stdout.write(self._prefix(workunit, s))

  def handle_message(self, workunit, *msg_elements):
    elements = [e if isinstance(e, basestring) else e[0] for e in msg_elements]
    sys.stdout.write(self._prefix(workunit, ''.join(elements)))

  def _format_aggregated_timings(self, aggregated_timings):
    return '\n'.join(['%(timing).3f %(label)s' % x for x in aggregated_timings.get_all()])

  def _format_artifact_cache_stats(self, artifact_cache_stats):
    stats = artifact_cache_stats.get_all()
    return 'Artifact cache reads not enabled.' if not stats else \
    '\n'.join(['%(cache_name)s - Hits: %(num_hits)d Misses: %(num_misses)d' % x
               for x in stats])

  def _prefix(self, workunit, s, with_time_string=False):
    if self._indenting:
      indent = '  ' * (len(workunit.ancestors()) - 1)
      return self._time_string(workunit, with_time_string) + ' ' + \
           ('\n' + ' ' * 14 + ' ').join([indent + line for line in s.strip().split('\n')])
    else:
      return self._time_string(workunit, with_time_string) + ' ' + s


  def _time_string(self, workunit, with_time_string):
    if with_time_string:
      return '\n' + workunit.start_time_string() + ' ' + workunit.start_delta_string()
    else:
      return '\n' + ' ' * 14

