import cgi
import os
import re


from twitter.pants import get_buildroot
from twitter.pants.base.build_file import BuildFile
from twitter.pants.reporting.renderer import Renderer


def _get_workunit_hierarchy(workunit):
  """Returns a list of this workunit and those enclosing it, up to but NOT including the root."""
  ret = []
  while workunit.parent is not None:  # Skip the root scope.
    ret.append(workunit)
    workunit = workunit.parent
  return list(reversed(ret))

def _get_scope_names(workunit):
  return [w.name for w in _get_workunit_hierarchy(workunit)]

class Formatter(object):
  def format(self, workunit, s):
    raise NotImplementedError('format() not implemented')

  def header(self):
    return ''

  def footer(self):
    return ''

  def start_workunit(self, workunit):
    scopes = _get_scope_names(workunit)
    if len(scopes) == 0:
      return ''
    return '[%s]\n' % ':'.join(scopes)

  def end_workunit(self, workunit):
    return ''


class PlainTextFormatter(Formatter):
  def format(self, workunit, s):
    return s


class HTMLFormatter(Formatter):
  def __init__(self, template_dir):
    self._renderer = Renderer(template_dir)
    self._buildroot = get_buildroot()

  def format(self, workunit, s):
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
  path_re = re.compile(r'(https?://)?/?(?:\w|[-.])+(?:/(?:\w|[-.])+)+(:w|[-.]+)?\w')  # At least two path components.
  def _linkify(self, s):
    def to_url(m):
      path = m.group(0)
      if m.group(1):
        return s  # It's an http(s) url.
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
      return '/browse/%s' % path

    return HTMLFormatter.path_re.sub(lambda m: '<a href="%s">%s</a>' % (to_url(m), m.group(0)), s)

  def start_workunit(self, workunit):
    if workunit.parent is None:  # We don't visualize the root of the tree.
      return ''
    scopes = _get_scope_names(workunit)
    args = { 'indent':len(scopes) * 10,
             'workunit': workunit_to_dict(workunit) }
    if workunit.type.endswith('_tool'):
      return self._renderer.render('tool_invocation_start', args)
    else:
      args.update({'header_text': ':'.join(scopes)})
      return self._renderer.render('report_scope_start', args)

  _status_classes = ['failure', 'warning', 'success', 'unknown']

  def end_workunit(self, workunit):
    if workunit.parent is None:  # We don't visualize the root of the tree.
      return ''
    args = { 'workunit': workunit_to_dict(workunit),
             'status': HTMLFormatter._status_classes[workunit.get_outcome()] }
    if workunit.type.endswith('_tool'):
      return self._renderer.render('tool_invocation_end', args)
    else:
      return self._renderer.render('report_scope_end', args)

def workunit_to_dict(workunit):
  """Because mustache doesn't seem to play nicely with objects."""
  ret = {}
  for key in ['parent', 'type', 'name', 'cmd', 'id']:
    ret[key] = getattr(workunit, key)
  return ret
