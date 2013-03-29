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
    return '%s [%s]\n' % (workunit.start_time_string(), workunit.get_path())

  def format_output(self, workunit, label, s):
    """Format captured output from an external tool."""
    return s

  def format_message(self, workunit, s):
    """Format an internal pants report message."""
    return s

  def format_targets(self, workunit, parts):
    """Format the list of target partitions."""
    return ''

  def end_workunit(self, workunit):
    return ''

  def format_aggregated_timings(self, workunit):
    """Format the list of aggregating timings for the workunit and everything under it."""
    return ''


class _PlainTextFormatter(Formatter):
  def start_workunit(self, workunit):
    return self.prefix(workunit, '[%s]' % workunit.name, with_timestamp=True) + '\n'

  def format_output(self, workunit, label, s):
    """Format captured output from an external tool."""
    return self.prefix(workunit, s)

  def format_message(self, workunit, s):
    """Format an internal pants report message."""
    return self.prefix(workunit, s)

  def format_targets(self, workunit, parts):
    num_partitions = len(parts)
    num_targets = 0
    num_files = 0
    for part in parts:
      for addr, n in part:
        num_targets += 1
        num_files += n
    s = 'Operating on '
    if num_files > 0:
      s += '%d files in ' % num_files
    s += '%d invalidated targets' % num_targets
    if num_partitions > 1:
      s += ' in %d target partitions' % num_partitions
    return self.prefix(workunit, s) + '.\n'

  def prefix(self, workunit, s, with_timestamp=False):
    raise NotImplementedError()

class IndentingPlainTextFormatter(_PlainTextFormatter):
  def start_workunit(self, workunit):
    return self.prefix(workunit, '[%s]' % workunit.name, with_timestamp=True) + '\n'

  def prefix(self, workunit, s, with_timestamp=False):
    indent = '  ' * (len(workunit.ancestors()) - 1)
    return (workunit.start_time_string() if with_timestamp else ' ' * 8) + ' ' + \
           '\n'.join([indent + line for line in s.split('\n')])


class NonIndentingPlainTextFormatter(_PlainTextFormatter):
  def start_workunit(self, workunit):
    return self.prefix(workunit, '[%s]' % workunit.get_path(), with_timestamp=True) + '\n'

  def prefix(self, workunit, s, with_timestamp=False):
    return (workunit.start_time_string() if with_timestamp else ' ' * 8) + ' ' + s


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

  def format_message(self, workunit, s):
    return self._append_to_workunit(workunit, self._htmlify_text(s))

  def format_targets(self, workunit, parts):
    num_partitions = len(parts)
    num_files = 0
    addrs = []
    for part in parts:
      for addr, n in part:
        addrs.append(addr)
        num_files += n
    addrs_txt = self._htmlify_text('\n'.join(addrs))
    args = {
      'id': workunit.id,
      'addrs': addrs_txt,
      'partitioned': num_partitions > 1,
      'num_partitions': num_partitions,
      'num_targets': len(addrs),
      'num_files': num_files
    }
    return self._append_to_workunit(workunit, self._renderer.render_name('targets', args))

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

