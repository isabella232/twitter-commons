import glob
import mimetypes
import os
import pystache
import sys
import urllib
import urlparse
import BaseHTTPServer

from collections import namedtuple

from twitter.pants.reporting.renderer import Renderer


class FileRegionHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  """A handler that serves regions of files under a given root:

  /browse/path/to/file?s=x&e=y serves from position x (inclusive) to position y (exclusive).
  /browse/path/to/file?s=x serves from position x (inclusive) until the end of the file.
  /browse/path/to/file serves the entire file.
  """
  Settings = namedtuple('Settings', ['renderer', 'assets_dir', 'root', 'allowed_clients'])

  def __init__(self, settings, request, client_address, server):
    self._root = settings.root
    self._renderer = settings.renderer
    self._assets_dir = settings.assets_dir
    self._allowed_clients = set(settings.allowed_clients)
    self._client_address = client_address
    BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, request, client_address, server)

  def _send_content(self, content, content_type, code=200):
    self.send_response(code)
    self.send_header('Content-Type', content_type)
    self.send_header('Content-Length', str(len(content)))
    self.end_headers()
    self.wfile.write(content)

  def do_GET(self):
    client_ip = self._client_address[0]
    if not client_ip in self._allowed_clients and not 'ALL' in self._allowed_clients:
      self._send_content('Access from host %s forbidden.' % client_ip, 'text/html')
      return

    try:
      (_, _, path, query, _) = urlparse.urlsplit(self.path)
      params = urlparse.parse_qs(query)
      if path.startswith('/browse/'):
        relpath = path[8:]
        abspath = os.path.normpath(os.path.join(self._root, relpath))
        if not abspath.startswith(self._root):
          raise ValueError  # Prevent using .. to get files from anywhere other than root.
        if os.path.isdir(abspath):
          self._serve_dir(abspath, params)
        elif os.path.isfile(abspath):
          self._serve_file(abspath, params)
      elif path.startswith('/content/'):
        relpath = path[9:]
        abspath = os.path.normpath(os.path.join(self._root, relpath))
        self._serve_file_content(abspath, params)
      elif path.startswith('/assets/'):
        relpath = path[8:]
        abspath = os.path.normpath(os.path.join(self._assets_dir, relpath))
        self._serve_asset(abspath)
    except (IOError, ValueError):
      sys.stderr.write('Invalid request %s' % self.path)

  def _serve_dir(self, abspath, params):
    relpath = os.path.relpath(abspath, self._root)
    breadcrumbs = self._create_breadcrumbs(relpath)
    entries = [ {'link_path': os.path.join(relpath, e), 'name': e} for e in os.listdir(abspath)]
    args = self._default_template_args('dir')
    args.update({ 'root_parent': os.path.dirname(self._root),
                  'breadcrumbs': breadcrumbs,
                  'entries': entries,
                  'params': params })
    self._send_content(self._renderer.render('base', args), 'text/html')

  def _serve_file(self, abspath, params):
    relpath = os.path.relpath(abspath, self._root)
    breadcrumbs = self._create_breadcrumbs(relpath)
    link_path = urlparse.urlunparse([None, None, relpath, None, urllib.urlencode(params), None])
    args = self._default_template_args('file')
    args.update({ 'root_parent': os.path.dirname(self._root),
                  'breadcrumbs': breadcrumbs,
                  'link_path': link_path })
    self._send_content(self._renderer.render('base', args), 'text/html')

  def _create_breadcrumbs(self, relpath):
    if relpath == '.':
      breadcrumbs = []
    else:
      path_parts = [os.path.basename(self._root)] + relpath.split(os.path.sep)
      path_links = ['/'.join(path_parts[1:i+1]) for i, name in enumerate(path_parts)]
      breadcrumbs = [{'link_path': link_path, 'name': name } for link_path, name in zip(path_links, path_parts)]
    return breadcrumbs

  def _serve_file_content(self, abspath, params):
    start = int(params.get('s')[0]) if 's' in params else 0
    end = int(params.get('e')[0]) if 'e' in params else None
    with open(abspath, 'r') as infile:
      if start:
        infile.seek(start)
      content = infile.read(end - start) if end else infile.read()
    content_type = mimetypes.guess_type(abspath)[0] or 'text/plain'
    if not content_type.startswith('text/'):
      content = repr(content)[1:-1]  # Will escape non-printables etc. We don't take the surrounding quotes.
      n = 120  # Split into lines of this size.
      content = '\n'.join([content[i:i+n] for i in xrange(0, len(content), n)])
      prettyprint = False
      prettyprint_js_files = []
    else:
      prettyprint = True
      prettyprint_js_files = [ {'name': x} for x in \
        filter(lambda x: x.endswith('.js'), os.listdir(os.path.join(self._assets_dir, 'js', 'prettify'))) ]
    linenums = True
    args = { 'prettyprint_js_files': prettyprint_js_files, 'content': content,
             'prettyprint': prettyprint, 'linenums': linenums }
    self._send_content(self._renderer.render('file_content', args), 'text/html')


  def _serve_asset(self, abspath):
    content_type = mimetypes.guess_type(abspath)[0] or 'text/plain'
    with open(abspath, 'r') as infile:
      content = infile.read()
    self._send_content(content, content_type)

  def _default_template_args(self, content_template):
    def include(text, args):
      template_name = pystache.render(text, args)
      return self._renderer.render(template_name, args)
    ret = { 'content_template': content_template }
    ret['include'] = lambda text: include(text, ret)
    return ret

  def log_message(self, format, *args):  # Silence BaseHTTPRequestHandler's logging.
    pass

class ReportingServer(object):
  def __init__(self, port, template_dir, assets_dir, root, allowed_clients):
    renderer = Renderer(template_dir, require=['base'])

    class MyHandler(FileRegionHandler):
      def __init__(self, request, client_address, server):
        settings = FileRegionHandler.Settings(renderer=renderer, assets_dir=assets_dir,
                                              root=root, allowed_clients=allowed_clients)
        FileRegionHandler.__init__(self, settings, request, client_address, server)

    self._httpd = BaseHTTPServer.HTTPServer(('', port), MyHandler)
    self._httpd.timeout = 0.1  # Not the network timeout, but how often handle_request yields.

  def start(self, run_before_blocking=list()):
    for f in run_before_blocking:
      f()
    self._httpd.serve_forever()

