import itertools
import json
import mimetypes
import os
import pystache
import re
import sys
import urllib
import urlparse

import BaseHTTPServer

from collections import namedtuple
from datetime import date, datetime

from pystache import Renderer

from twitter.pants.goal.run_tracker import RunInfo


# Prettyprint plugin files.
PPP_RE=re.compile("""^lang-.*\.js$""")

Settings = namedtuple('Settings',
  ['info_dir', 'reports_dir', 'template_dir', 'assets_dir', 'root', 'allowed_clients'])


class PantsHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  """A handler that serves regions of files under a given root:

  /browse/path/to/file?s=x&e=y serves from position x (inclusive) to position y (exclusive).
  /browse/path/to/file?s=x serves from position x (inclusive) until the end of the file.
  /browse/path/to/file serves the entire file.
  """
  def __init__(self, settings, renderer, request, client_address, server):
    self._settings = settings
    self._root = self._settings.root
    self._renderer = renderer
    self._client_address = client_address
    self._GET_handlers = [
      ('/runs/', self._handle_runs),
      ('/browse/', self._handle_browse),
      ('/content/', self._handle_content),
      ('/assets/', self._handle_assets),
      ('/poll', self._handle_poll),
      ('/latestrunid', self._handle_latest_runid)
    ]
    BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, request, client_address, server)

  def _send_content(self, content, content_type, code=200):
    self.send_response(code)
    self.send_header('Content-Type', content_type)
    self.send_header('Content-Length', str(len(content)))
    self.end_headers()
    self.wfile.write(content)

  def do_GET(self):
    if not self._client_allowed():
      return

    try:
      (_, _, path, query, _) = urlparse.urlsplit(self.path)
      params = urlparse.parse_qs(query)
      for prefix, handler in self._GET_handlers:
        if self._maybe_handle(prefix, handler, path, params):
          return
      if path == '/':  # Show runs by default.
        self._handle_runs('', {})
      self._send_content('Invalid GET request %s' % self.path, 'text/html')
    except (IOError, ValueError):
      sys.stderr.write('Invalid GET request %s' % self.path)

  def _client_allowed(self):
    client_ip = self._client_address[0]
    if not client_ip in self._settings.allowed_clients and not 'ALL' in self._settings.allowed_clients:
      self._send_content('Access from host %s forbidden.' % client_ip, 'text/html')
      return False
    return True

  def _maybe_handle(self, prefix, handler, path, params, data=None):
    if path.startswith(prefix):
      relpath = path[len(prefix):]
      if data:
        handler(relpath, params, data)
      else:
        handler(relpath, params)
      return True
    else:
      return False

  def _handle_runs(self, relpath, params):
    if relpath == '':
      # Show a listing of all runs since the last clean-all.
      runs_by_day = self._partition_runs_by_day()
      args = self._default_template_args('run_list')
      args['runs_by_day'] = runs_by_day
    else:
      # Show the report for a specific run.
      args = self._default_template_args('run')
      run_id = relpath
      run_info = self._get_run_info_dict(run_id)
      if run_info is None:
        args['no_such_run'] = relpath
        if run_id == 'latest':
          args['is_latest'] = 'none'
      else:
        report_abspath = run_info['default_report']
        report_relpath = os.path.relpath(report_abspath, self._root)
        timings_path = os.path.join(os.path.dirname(report_relpath), 'aggregated_timings')
        run_info['timestamp_text'] = \
          datetime.fromtimestamp(float(run_info['timestamp'])).strftime('%H:%M:%S on %A, %B %d %Y')
        args.update({'run_info': run_info,
                     'report_path': report_relpath,
                     'aggregated_timings_path': timings_path })
        if run_id == 'latest':
          args['is_latest'] = run_info['id']
        args.update({ 'collapsible': lambda x: self._render_callable('collapsible', x, args) })
    self._send_content(self._renderer.render_name('base', args), 'text/html')

  def _render_callable(self, template_name, arg_string, outer_args):
    rendered_arg_string = self._renderer.render(arg_string, outer_args)
    inner_args = dict([(k, v[0]) for k, v in urlparse.parse_qs(rendered_arg_string).items()])
    return self._renderer.render_name(template_name, inner_args)

  def _handle_browse(self, relpath, params):
    abspath = os.path.normpath(os.path.join(self._root, relpath))
    if not abspath.startswith(self._root):
      raise ValueError  # Prevent using .. to get files from anywhere other than root.
    if os.path.isdir(abspath):
      self._serve_dir(abspath, params)
    elif os.path.isfile(abspath):
      self._serve_file(abspath, params)

  def _handle_content(self, relpath, params):
    abspath = os.path.normpath(os.path.join(self._root, relpath))
    if os.path.isfile(abspath):
      with open(abspath, 'r') as infile:
        content = infile.read()
    else:
      content = 'No file found at %s' % abspath
    content_type = mimetypes.guess_type(abspath)[0] or 'text/plain'
    if not content_type.startswith('text/') and not content_type == 'application/xml':
      # Binary file, split it into lines.
      n = 120  # Display lines of this max size.
      content = repr(content)[1:-1]  # Will escape non-printables etc. We don't take the surrounding quotes.
      content = '\n'.join([content[i:i+n] for i in xrange(0, len(content), n)])
      prettify = False
      prettify_extra_langs = []
    else:
      prettify = True
      prettify_extra_langs =\
      [ {'name': x} for x in os.listdir(os.path.join(self._settings.assets_dir, 'js', 'prettify_extra_langs')) ]
    linenums = True
    args = { 'prettify_extra_langs': prettify_extra_langs, 'content': content,
             'prettify': prettify, 'linenums': linenums }
    self._send_content(self._renderer.render_name('file_content', args), 'text/html')

  def _handle_assets(self, relpath, params):
    abspath = os.path.normpath(os.path.join(self._settings.assets_dir, relpath))
    content_type = mimetypes.guess_type(abspath)[0] or 'text/plain'
    with open(abspath, 'r') as infile:
      content = infile.read()
    self._send_content(content, content_type)

  def _handle_poll(self, relpath, params):
    request = json.loads(params.get('q')[0])
    ret = {}
    # request is a polling request for multiple files. For each file:
    #  - id is some identifier assigned by the client, used to differentiate the results.
    #  - path is the file to poll.
    #  - pos is the last byte position in that file seen by the client.
    for poll in request:
      id = poll.get('id', None)
      path = poll.get('path', None)
      pos = poll.get('pos', 0)
      if path:
        abspath = os.path.normpath(os.path.join(self._root, path))
        if os.path.isfile(abspath):
          with open(abspath, 'r') as infile:
            if pos:
              infile.seek(pos)
            content = infile.read()
            ret[id] = content
    self._send_content(json.dumps(ret), 'application/json')

  def _handle_latest_runid(self, relpath, params):
    latest_runinfo = self._get_run_info_dict('latest')
    if latest_runinfo is None:
      self._send_content('none', 'text/plain')
    else:
      self._send_content(latest_runinfo['id'], 'text/plain')

  def _partition_runs_by_day(self):
    run_infos = self._get_all_run_infos()
    for x in run_infos:
      ts = float(x['timestamp'])
      x['time_of_day_text'] = datetime.fromtimestamp(ts).strftime('%H:%M:%S')

    def date_text(dt):
      delta_days = (date.today() - dt).days
      if delta_days == 0:
        return 'Today'
      elif delta_days == 1:
        return 'Yesterday'
      elif delta_days < 7:
        return dt.strftime('%A')  # Weekday name.
      else:
        d = dt.day % 10
        suffix = 'st' if d == 1 else 'nd' if d == 2 else 'rd' if d == 3 else 'th'
        return dt.strftime('%B %d') + suffix  # E.g., October 30th.

    keyfunc = lambda x: datetime.fromtimestamp(float(x['timestamp']))
    sorted_run_infos = sorted(run_infos, key=keyfunc, reverse=True)
    return [ { 'date_text': date_text(dt), 'run_infos': [x for x in infos] }
             for dt, infos in itertools.groupby(sorted_run_infos, lambda x: keyfunc(x).date()) ]

  def _get_run_info_dict(self, run_id):
    run_info_path = os.path.join(self._settings.info_dir, run_id) + '.info'
    if os.path.exists(run_info_path):
      # We copy the RunInfo as a dict, so we can add stuff to it to pass to the template.
      return RunInfo(run_info_path).get_as_dict()
    else:
      return None

  def _get_all_run_infos(self):
    info_dir = self._settings.info_dir
    if not os.path.isdir(info_dir):
      return []
    # We copy the RunInfo as a dict, so we can add stuff to it to pass to the template.
    return [RunInfo(os.path.join(info_dir, x)).get_as_dict()
            for x in os.listdir(info_dir)
            if x.endswith('.info') and not os.path.islink(os.path.join(info_dir, x))]

  def _serve_dir(self, abspath, params):
    relpath = os.path.relpath(abspath, self._root)
    breadcrumbs = self._create_breadcrumbs(relpath)
    entries = [ {'link_path': os.path.join(relpath, e), 'name': e} for e in os.listdir(abspath)]
    args = self._default_template_args('dir')
    args.update({ 'root_parent': os.path.dirname(self._root),
                  'breadcrumbs': breadcrumbs,
                  'entries': entries,
                  'params': params })
    self._send_content(self._renderer.render_name('base', args), 'text/html')

  def _serve_file(self, abspath, params):
    relpath = os.path.relpath(abspath, self._root)
    breadcrumbs = self._create_breadcrumbs(relpath)
    link_path = urlparse.urlunparse([None, None, relpath, None, urllib.urlencode(params), None])
    args = self._default_template_args('file')
    args.update({ 'root_parent': os.path.dirname(self._root),
                  'breadcrumbs': breadcrumbs,
                  'link_path': link_path })
    self._send_content(self._renderer.render_name('base', args), 'text/html')

  def _create_breadcrumbs(self, relpath):
    if relpath == '.':
      breadcrumbs = []
    else:
      path_parts = [os.path.basename(self._root)] + relpath.split(os.path.sep)
      path_links = ['/'.join(path_parts[1:i+1]) for i, name in enumerate(path_parts)]
      breadcrumbs = [{'link_path': link_path, 'name': name } for link_path, name in zip(path_links, path_parts)]
    return breadcrumbs

  def _default_template_args(self, content_template):
    def include(text, args):
      template_name = pystache.render(text, args)
      return self._renderer.render_name(template_name, args)
    ret = { 'content_template': content_template }
    ret['include'] = lambda text: include(text, ret)
    return ret

  def log_message(self, format, *args):  # Silence BaseHTTPRequestHandler's logging.
    pass

class ReportingServer(object):
  def __init__(self, port, settings):
    renderer = Renderer(search_dirs=settings.template_dir)

    class MyHandler(PantsHandler):
      def __init__(self, request, client_address, server):
        PantsHandler.__init__(self, settings, renderer, request, client_address, server)

    self._httpd = BaseHTTPServer.HTTPServer(('', port), MyHandler)
    self._httpd.timeout = 0.1  # Not the network timeout, but how often handle_request yields.

  def start(self, run_before_blocking=list()):
    for f in run_before_blocking:
      f()
    self._httpd.serve_forever()

