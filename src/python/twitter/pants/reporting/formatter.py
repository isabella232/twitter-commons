import cgi
import os
import re

from twitter.pants import get_buildroot
from twitter.pants.base.build_file import BuildFile
from twitter.pants.reporting.renderer import Renderer


class Formatter(object):
  def format(self, s):
    raise NotImplementedError('format() not implemented')

  def header(self):
    return ''

  def footer(self):
    return ''

  def enter_scope(self, workunit):
    scopes = workunit.get_reporting_names()
    if len(scopes) == 0:
      return ''
    return '[%s]\n' % ':'.join(scopes)

  def exit_scope(self, workunit):
    return ''


class PlainTextFormatter(Formatter):
  def format(self, s):
    return s


class HTMLFormatter(Formatter):
  def __init__(self, template_dir):
    self._renderer = Renderer(template_dir)
    self.buildroot = get_buildroot()

  def format(self, s):
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
        path = os.path.relpath(path, self.buildroot)
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

  def header(self):
    return ''

  def footer(self):
    return ''

  def enter_scope(self, workunit):
    scopes = workunit.get_reporting_names()
    if len(scopes) == 0:  # We don't visualize the root of the tree.
      return ''
    parent_scopes = scopes[:-1]
    args = { 'indent':len(scopes) * 10,
             'scope_id': self._scope_id(scopes),
             'parent_scope_id': self._scope_id(parent_scopes),
             'header_text': ':'.join(scopes)}
    return self._renderer.render('report_scope_start', args)

  _status_classes = ['failure', 'warning', 'success', 'unknown']

  def exit_scope(self, workunit):
    scopes = workunit.get_reporting_names()
    if len(scopes) == 0: # We don't visualize the root of the tree.
      return ''
    args = { 'scope_id': self._scope_id(scopes),
             'status': HTMLFormatter._status_classes[workunit.get_outcome()] }
    return self._renderer.render('report_scope_end', args)

  def _scope_id(self, scopes):
    return 'scope-' + '-'.join(scopes)
