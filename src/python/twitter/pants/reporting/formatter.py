import cgi
import os
import re
import urlparse
import uuid

from pystache import Renderer

from twitter.pants import get_buildroot
from twitter.pants.base.build_file import BuildFile
from twitter.pants.base.mustache import MustacheRenderer
from twitter.pants.goal.work_unit import WorkUnit


class Formatter(object):
  def start_run(self):
    return ''

  def end_run(self):
    return ''

  def start_workunit(self, workunit):
    return ''

  def format_output(self, workunit, label, s):
    """Format captured output from an external tool."""
    return s

  def format_message(self, workunit, *msg_elements):
    """Format an internal pants report message."""
    return ''.join(msg_elements)

  def end_workunit(self, workunit):
    return ''

  def format_aggregated_timings(self, aggregated_timings):
    """Format the list of aggregating timings for the workunit and everything under it."""
    return ''

  def format_artifact_cache_stats(self, artifact_cache_stats):
    """Format the artifact cache stats."""
    return ''


class _PlainTextFormatter(Formatter):
  def end_run(self):
    return '\n'

  def format_output(self, workunit, label, s):
    """Format captured output from an external tool."""
    return self.prefix(workunit, s)

  def format_message(self, workunit, *msg_elements):
    """Format an internal pants report message."""
    elements = [e if isinstance(e, basestring) else e[0] for e in msg_elements]
    return self.prefix(workunit, ''.join(elements))

  def format_aggregated_timings(self, aggregated_timings):
    return '\n'.join(['%(timing).3f %(label)s' % x for x in aggregated_timings.get_all()])

  def format_artifact_cache_stats(self, artifact_cache_stats):
    stats = artifact_cache_stats.get_all()
    return 'Artifact cache reads not enabled.' if not stats else \
           '\n'.join(['%(cache_name)s - Hits: %(num_hits)d Misses: %(num_misses)d' % x
                     for x in stats])

  def time_string(self, workunit, with_time_string):
    if with_time_string:
      return '\n' + workunit.start_time_string() + ' ' + workunit.start_delta_string()
    else:
      return '\n' + ' ' * 14

  def prefix(self, workunit, s, with_time_string=False):
    raise NotImplementedError()


class IndentingPlainTextFormatter(_PlainTextFormatter):
  def start_workunit(self, workunit):
    if workunit.parent and workunit.parent.is_multitool():
      return '.'
    return self.prefix(workunit, '[%s]' % workunit.name, with_time_string=True)

  def prefix(self, workunit, s, with_time_string=False):
    indent = '  ' * (len(workunit.ancestors()) - 1)
    return self.time_string(workunit, with_time_string) + ' ' + \
           ('\n' + ' ' * 14 + ' ').join([indent + line for line in s.strip().split('\n')])


class NonIndentingPlainTextFormatter(_PlainTextFormatter):
  def start_workunit(self, workunit):
    if workunit.parent and workunit.parent.is_multitool():
      return '.'
    return self.prefix(workunit, '[%s]' % workunit.get_path(), with_time_string=True)

  def prefix(self, workunit, s, with_time_string=False):
    return self.time_string(workunit, with_time_string) + ' ' + s


class HTMLFormatter(Formatter):
  def __init__(self, template_dir, html_dir):
    Formatter.__init__(self)
    self._renderer = MustacheRenderer(Renderer(search_dirs=template_dir))
    self._buildroot = get_buildroot()
    self._html_path_base = os.path.relpath(html_dir, self._buildroot)

  def start_workunit(self, workunit):
    is_tool = workunit.is_tool()
    is_multitool = workunit.is_multitool()
    if workunit.parent is None:
      header_text = 'all'
    else:
      header_text = workunit.name
    workunit_dict = workunit.to_dict()
    if workunit_dict['cmd']:
      workunit_dict['cmd'] = self._linkify(workunit_dict['cmd'])
    args = { 'indent': len(workunit.ancestors()) * 10,
             'html_path_base': self._html_path_base,
             'workunit': workunit_dict,
             'header_text': header_text,
             'initially_open': not (is_tool or is_multitool),
             'is_tool': is_tool,
             'is_multitool': is_multitool }
    args.update({ 'collapsible': lambda x: self._render_callable('collapsible', x, args) })

    ret = self._renderer.render_name('workunit_start', args)
    if is_tool:
      ret += self._renderer.render_name('tool_invocation_start', args)
    return ret

  _status_css_classes = ['aborted', 'failure', 'warning', 'success', 'unknown']

  def format_output(self, workunit, label, s):
    return self._htmlify_text(s)

  def format_message(self, workunit, *msg_elements):
    elements = []
    detail_ids = []
    for e in msg_elements:
      if isinstance(e, basestring):
        elements.append({'text': self._htmlify_text(e)})
      else:  # Assume it's a pair (text, detail).
        detail_id = uuid.uuid4()
        detail_ids.append(detail_id)
        elements.append({'text': self._htmlify_text(e[0]),
                         'detail': self._htmlify_text(e[1]),
                         'detail-id': detail_id })
    args = { 'elements': elements,
             'detail-ids': detail_ids }
    return self._append_to_workunit(workunit, self._renderer.render_name('message', args))

  def end_workunit(self, workunit):
    duration = workunit.duration()
    timing = '%.3f' % duration
    unaccounted_time_secs = workunit.unaccounted_time()
    unaccounted_time = '%.3f' % unaccounted_time_secs \
      if unaccounted_time_secs >= 1 and unaccounted_time_secs > 0.05 * duration \
      else None
    args = { 'workunit': workunit.to_dict(),
             'status': workunit.choose(*HTMLFormatter._status_css_classes),
             'timing': timing,
             'unaccounted_time': unaccounted_time,
             'aborted': workunit.outcome() == WorkUnit.ABORTED }

    ret = ''
    if workunit.type.endswith('_tool'):
      ret += self._renderer.render_name('tool_invocation_end', args)
    return ret + self._renderer.render_name('workunit_end', args)

  def format_aggregated_timings(self, aggregated_timings):
    aggregated_timings_dict = aggregated_timings.get_all()
    for item in aggregated_timings_dict:
      item['timing_string'] = '%.3f' % item['timing']
    args = {
      'timings': aggregated_timings_dict
    }
    return self._renderer.render_name('aggregated_timings', args)

  def format_artifact_cache_stats(self, artifact_cache_stats):
    args = {
      'artifact_cache_stats': artifact_cache_stats.get_all()
    }
    return self._renderer.render_name('artifact_cache_stats', args)

  def _render_callable(self, template_name, arg_string, outer_args):
    rendered_arg_string = self._renderer.render(arg_string, outer_args)
    inner_args = dict([(k, v[0]) for k, v in urlparse.parse_qs(rendered_arg_string).items()])
    args = dict(inner_args.items() + outer_args.items())
    return self._renderer.render_name(template_name, args)

  def _htmlify_text(self, s):
    colored = self._handle_ansi_color_codes(cgi.escape(s))
    return self._linkify(colored).replace('\n', '</br>')

  def _append_to_workunit(self, workunit, s):
    args = {
      'output_id': uuid.uuid4(),
      'workunit_id': workunit.id,
      'str': s,
      }
    return self._renderer.render_name('output', args)

  # Replace ansi color sequences with spans of appropriately named css classes.
  ansi_color_code_re = re.compile(r'\033\[((?:\d|;)*)m')
  def _handle_ansi_color_codes(self, s):
    def ansi_code_to_css(code):
      return ' '.join(['ansi-%s' % c for c in code.split(';')])
    return '<span>' + \
           HTMLFormatter.ansi_color_code_re.sub(
             lambda m: '</span><span class="%s">' % ansi_code_to_css(m.group(1)), s) + \
           '</span>'

  # Heuristics to find and linkify file and http references.
  # We require no trailing dots because some tools print an ellipsis after file names
  # (I'm looking at you, zinc). None of our files end in a dot in practice, so this is fine.

  # At least two path components.
  path_re = re.compile(r'(https?://)?/?(?:\w|[-.])+(?:/(?:\w|[-.])+)+(:w|[-.]+)?\w')

  def _linkify(self, s):
    def to_url(m):
      if m.group(1):
        return m.group(0)  # It's an http(s) url.
      path = m.group(0)
      if path.startswith('/'):
        path = os.path.relpath(path, self._buildroot)
      else:
        # See if it's a reference to a target in a BUILD file.
        # TODO: Deal with sibling BUILD files?
        parts = path.split(':')
        if len(parts) == 2:
          putative_dir = parts[0]
        else:
          putative_dir = path
        if os.path.isdir(putative_dir):
          path = os.path.join(putative_dir, BuildFile._CANONICAL_NAME)
      if os.path.exists(os.path.join(self._buildroot, path)):
        return '/browse/%s' % path
      else:
        return None

    def maybe_add_link(url, text):
      return '<a target="_blank" href="%s">%s</a>' % (url, text) if url else text
    return HTMLFormatter.path_re.sub(lambda m: maybe_add_link(to_url(m), m.group(0)), s)

