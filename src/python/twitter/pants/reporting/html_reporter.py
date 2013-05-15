import cgi
import os
import re
import urlparse
import uuid
from pystache.renderer import Renderer

from twitter.common.dirutil import safe_mkdir
from twitter.pants import get_buildroot
from twitter.pants.base.build_file import BuildFile
from twitter.pants.base.mustache import MustacheRenderer
from twitter.pants.goal.workunit import WorkUnit
from twitter.pants.reporting.reporter import Reporter
from twitter.pants.reporting.reporting_utils import list_to_report_element


# A regex to recognize substrings that are probably URLs or file paths. Broken down for readability.
_PREFIX = r'(https?://)?/?' # http://, https:// or / or nothing.
_REL_PATH_COMPONENT = r'(\w|[-.])+'  # One or more alphanumeric, underscore, dash or dot.
_ABS_PATH_COMPONENT = '/' + _REL_PATH_COMPONENT
_ABS_PATH_COMPONENTS = '(%s)+' % _ABS_PATH_COMPONENT
_OPTIONAL_TARGET_SUFFIX = '(:%s)?' % _REL_PATH_COMPONENT  # For /foo/bar:target.

# Note that we require at least two path components.
# We require no trailing dots because some tools print an ellipsis after file names
# (I'm looking at you, zinc). None of our files end in a dot in practice, so this is fine.
_PATH = _PREFIX + _REL_PATH_COMPONENT + _ABS_PATH_COMPONENTS + _OPTIONAL_TARGET_SUFFIX + '\w'
_PATH_RE = re.compile(_PATH)

def linkify(buildroot, s):
  """Augment text by heuristically finding URL and file references and turning them into links/"""
  def to_url(m):
    if m.group(1):
      return m.group(0)  # It's an http(s) url.
    path = m.group(0)
    if path.startswith('/'):
      path = os.path.relpath(path, buildroot)
    else:
      # See if it's a reference to a target in a BUILD file.
      # TODO: Deal with sibling BUILD files?
      parts = path.split(':')
      if len(parts) == 2:
        putative_dir = parts[0]
      else:
        putative_dir = path
      if os.path.isdir(os.path.join(buildroot, putative_dir)):
        path = os.path.join(putative_dir, BuildFile._CANONICAL_NAME)
    if os.path.exists(os.path.join(buildroot, path)):
      return '/browse/%s' % path
    else:
      return None

  def maybe_add_link(url, text):
    return '<a target="_blank" href="%s">%s</a>' % (url, text) if url else text
  return _PATH_RE.sub(lambda m: maybe_add_link(to_url(m), m.group(0)), s)


class HtmlReporter(Reporter):
  """HTML reporting to files.

  The files are intended to be served by the ReportingServer,
  not accessed directly from the filesystem.
  """

  def __init__(self, run_tracker, html_dir, template_dir):
    Reporter.__init__(self, run_tracker)
     # The main report, and associated tool outputs, go under this dir.
    self._html_dir = html_dir

    # We render HTML from mustache templates.
    self._renderer = MustacheRenderer(Renderer(search_dirs=template_dir))

    # We serve files relative to the build root.
    self._buildroot = get_buildroot()
    self._html_path_base = os.path.relpath(self._html_dir, self._buildroot)

    # We write the main report body to this file object.
    self._report_file = None

    # We redirect stdout, stderr etc. of tool invocations to these files.
    self._output_files = {}  # path -> fileobj.

  def report_path(self):
    """The path to the main report file."""
    return os.path.join(self._html_dir, 'build.html')

  def open(self):
    """Implementation of Reporter callback."""
    safe_mkdir(os.path.dirname(self._html_dir))
    self._report_file = open(self.report_path(), 'w')

  def close(self):
    """Implementation of Reporter callback."""
    self._report_file.close()
    for f in self._output_files.values():
      f.close()

  def start_workunit(self, workunit):
    """Implementation of Reporter callback."""
    # We use these properties of the workunit to decide how to render information about it.
    is_tool = workunit.has_label(WorkUnit.TOOL)
    is_multitool = workunit.has_label(WorkUnit.MULTITOOL)
    is_test = workunit.has_label(WorkUnit.TEST)

    if workunit.parent is None:
      header_text = 'all'
    else:
      header_text = workunit.name

    # Create the template arguments.
    workunit_dict = workunit.to_dict()
    if workunit_dict['cmd']:
      workunit_dict['cmd'] = linkify(self._buildroot, workunit_dict['cmd'].replace('$', '\\\\$'))
    args = { 'indent': len(workunit.ancestors()) * 10,
             'html_path_base': self._html_path_base,
             'workunit': workunit_dict,
             'header_text': header_text,
             'initially_open': is_test or not (is_tool or is_multitool),
             'is_tool': is_tool,
             'is_multitool': is_multitool }
    args.update({ 'collapsible': lambda x: self._render_callable('collapsible', x, args) })

    s = self._renderer.render_name('workunit_start', args)
    if is_tool:
      del args['initially_open']
      if is_test:  # We usually want to see test framework output.
        args['stdout_initially_open'] = True
      s += self._renderer.render_name('tool_invocation_start', args)
    self._emit(s)

  _status_css_classes = ['aborted', 'failure', 'warning', 'success', 'unknown']

  def end_workunit(self, workunit):
    """Implementation of Reporter callback."""
    duration = workunit.duration()
    timing = '%.3f' % duration
    unaccounted_time_secs = workunit.unaccounted_time()
    unaccounted_time = '%.3f' % unaccounted_time_secs \
      if unaccounted_time_secs >= 1 and unaccounted_time_secs > 0.05 * duration \
      else None
    args = { 'workunit': workunit.to_dict(),
             'status': workunit.choose(*HtmlReporter._status_css_classes),
             'timing': timing,
             'unaccounted_time': unaccounted_time,
             'aborted': workunit.outcome() == WorkUnit.ABORTED }

    s = ''
    if workunit.has_label(WorkUnit.TOOL):
      s += self._renderer.render_name('tool_invocation_end', args)
    s += self._renderer.render_name('workunit_end', args)
    self._emit(s)

    # Update the timings.
    def render_timings(timings):
      timings_dict = timings.get_all()
      for item in timings_dict:
        item['timing_string'] = '%.3f' % item['timing']
      args = {
        'timings': timings_dict
      }
      return self._renderer.render_name('aggregated_timings', args)

    self._overwrite('cumulative_timings', render_timings(self.run_tracker.cumulative_timings))
    self._overwrite('self_timings', render_timings(self.run_tracker.self_timings))

    # Update the artifact cache stats.
    def render_cache_stats(artifact_cache_stats):
      def set_explicit_detail_id(e, id):
        if isinstance(e, basestring):
          return e # No details, so nothing to do.
        else:
          return e + (False, id)

      msg_elements = []
      for cache_name, stat in artifact_cache_stats.stats_per_cache.items():
        msg_elements.extend([
          cache_name + ' artifact cache: ',
          # Explicitly set the detail ids, so we can check from JS whether they are visible.
          set_explicit_detail_id(list_to_report_element(stat.hit_targets, 'hit'),
                                 'cache-hit-details'),
          ', ',
          set_explicit_detail_id(list_to_report_element(stat.miss_targets, 'miss'),
                                 'cache-miss-details'),
          '.'
        ])
      if not msg_elements:
        msg_elements = ['No artifact cache use.']
      return self._render_message(*msg_elements)

    self._overwrite('artifact_cache_stats',
                    render_cache_stats(self.run_tracker.artifact_cache_stats))

  def handle_output(self, workunit, label, s):
    """Implementation of Reporter callback."""
    if os.path.exists(self._html_dir):  # Make sure we're not immediately after a clean-all.
      path = os.path.join(self._html_dir, '%s.%s' % (workunit.id, label))
      if path not in self._output_files:
        f = open(path, 'w')
        self._output_files[path] = f
      else:
        f = self._output_files[path]
      f.write(self._htmlify_text(s))
      # We must flush in the same thread as the write.
      f.flush()

  def handle_message(self, workunit, *msg_elements):
    """Implementation of Reporter callback."""
    s = self._append_to_workunit(workunit, self._render_message(*msg_elements))
    self._emit(s)

  def _render_message(self, *msg_elements):
    elements = []
    detail_ids = []
    for e in msg_elements:
      if isinstance(e, basestring):
        elements.append({'text': self._htmlify_text(e)})
      elif len(e) == 1:
        elements.append({'text': self._htmlify_text(e[0])})
      else:  # Assume it's a tuple (text, detail[, detail_initially_visible[, detail_id]])
        detail_initially_visible = e[2] if len(e) > 2 else False
        detail_id = e[3] if len(e) > 3 else uuid.uuid4()
        detail_ids.append(detail_id)
        elements.append({'text': self._htmlify_text(e[0]),
                         'detail': self._htmlify_text(e[1]),
                         'detail-id': detail_id,
                         'detail_initially_visible': detail_initially_visible })
    args = { 'elements': elements,
             'detail-ids': detail_ids }
    return self._renderer.render_name('message', args)

  def _emit(self, s):
    """Append content to the main report file."""
    if os.path.exists(self._html_dir):  # Make sure we're not immediately after a clean-all.
      self._report_file.write(s)
      self._report_file.flush()  # We must flush in the same thread as the write.

  def _overwrite(self, filename, s):
    """Overwrite a file with the specified contents."""
    if os.path.exists(self._html_dir):  # Make sure we're not immediately after a clean-all.
      with open(os.path.join(self._html_dir, filename), 'w') as f:
        f.write(s)

  def _append_to_workunit(self, workunit, s):
    args = {
      'output_id': uuid.uuid4(),
      'workunit_id': workunit.id,
      'str': s,
      }
    return self._renderer.render_name('output', args)

  def _render_callable(self, inner_template_name, arg_string, outer_args):
    """Handle a mustache callable.

    In a mustache template, when foo is callable, {{#foo}}arg_string{{/foo}} is replaced with the
    result of calling foo(arg_string). The callable must interpret arg_string.

    This method provides an implementation of such a callable that does the following:
      A) Parses the arg_string as CGI args.
      B) Adds them to the original args that the enclosing template was rendered with.
      C) Renders some other template against those args.
      D) Returns the resulting text.
    """
    # First render the arg_string (mustache doesn't do this for you).
    rendered_arg_string = self._renderer.render(arg_string, outer_args)
    # Parse the inner args as CGI args.
    inner_args = dict([(k, v[0]) for k, v in urlparse.parse_qs(rendered_arg_string).items()])
    # Order matters: lets the inner args override the outer args.
    args = dict(outer_args.items() + inner_args.items())
    # Render.
    return self._renderer.render_name(inner_template_name, args)

  def _htmlify_text(self, s):
    """Make text HTML-friendly."""
    colored = self._handle_ansi_color_codes(cgi.escape(s))
    return linkify(self._buildroot, colored).replace('\n', '</br>')

  _ANSI_COLOR_CODE_RE = re.compile(r'\033\[((\d|;)*)m')
  def _handle_ansi_color_codes(self, s):
    """Replace ansi color sequences with spans of appropriately named css classes."""
    def ansi_code_to_css(code):
      return ' '.join(['ansi-%s' % c for c in code.split(';')])
    return '<span>' +\
           HtmlReporter._ANSI_COLOR_CODE_RE.sub(
             lambda m: '</span><span class="%s">' % ansi_code_to_css(m.group(1)), s) +\
           '</span>'
