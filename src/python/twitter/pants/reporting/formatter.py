import cgi
import os
import re
import urlparse

from pystache import Renderer

from twitter.pants import get_buildroot
from twitter.pants.base.build_file import BuildFile
from twitter.pants.goal.work_unit import WorkUnit


class Formatter(object):
  def format(self, workunit, label, s):
    raise NotImplementedError('format() not implemented')

  def start_run(self):
    return ''

  def end_run(self):
    return ''

  def start_workunit(self, workunit):
    return '[%s]\n' % workunit.get_path()

  def end_workunit(self, workunit):
    return ''

  def format_aggregated_timings(self, workunit):
    return ''


class PlainTextFormatter(Formatter):
  def format(self, workunit, label, s):
    return s


class HTMLFormatter(Formatter):
  def __init__(self, template_dir, html_dir):
    self._renderer = Renderer(search_dirs=template_dir)
    self._buildroot = get_buildroot()
    self._html_path_base = os.path.relpath(html_dir, self._buildroot)

  def format(self, workunit, label, s):
    colored = self._handle_ansi_color_codes(cgi.escape(s))
    return self._linkify(colored).replace('\n', '</br>')

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

  def end_workunit(self, workunit):
    duration = workunit.duration()
    timing = '%.3f' % duration
    unaccounted_time_secs = workunit.unaccounted_time()
    unaccounted_time = '%.3f' % unaccounted_time_secs \
      if unaccounted_time_secs >= 1 and unaccounted_time_secs > 0.01 * duration \
      else None
    args = { 'workunit': workunit.to_dict(),
             'status': workunit.choose(*HTMLFormatter._status_css_classes),
             'timing': timing,
             'unaccounted_time': unaccounted_time,
             'aborted': workunit.get_outcome() == WorkUnit.ABORTED }

    ret = ''
    if workunit.type.endswith('_tool'):
      ret += self._renderer.render_name('tool_invocation_end', args)
    return ret + self._renderer.render_name('workunit_end', args)

  def format_aggregated_timings(self, workunit):
    aggregated_timings = workunit.aggregated_timings.get_all()
    for item in aggregated_timings:
      item['timing_string'] = '%.3f' % item['timing']
    args = {
      'timings': aggregated_timings
    }
    return self._renderer.render_name('aggregated_timings', args)

  def _render_callable(self, template_name, arg_string, outer_args):
    rendered_arg_string = self._renderer.render(arg_string, outer_args)
    inner_args = dict([(k, v[0]) for k, v in urlparse.parse_qs(rendered_arg_string).items()])
    return self._renderer.render_name(template_name, inner_args)
