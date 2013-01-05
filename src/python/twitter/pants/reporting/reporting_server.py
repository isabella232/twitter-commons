import mimetypes
import os
import pystache
import sys
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
      elif path.startswith('/assets/'):
        relpath = path[8:]
        abspath = os.path.normpath(os.path.join(self._assets_dir, relpath))
        self._serve_file(abspath, params)
    except (IOError, ValueError):
      sys.stderr.write('Invalid request %s' % self.path)

  def _serve_dir(self, abspath, params):
    relpath = os.path.relpath(abspath, self._root)
    if relpath == '.':
      breadcrumbs = []
    else:
      path_parts = [os.path.basename(self._root)] + relpath.split(os.path.sep)
      path_links = ['/'.join(path_parts[1:i+1]) for i, name in enumerate(path_parts)]
      breadcrumbs = [{'link_path': link_path, 'name': name } for link_path, name in zip(path_links, path_parts)]
    entries = [ {'link_path': os.path.join(relpath, e), 'name': e} for e in os.listdir(abspath)]
    args = self._default_template_args('dir')
    args.update({ 'root_parent': os.path.dirname(self._root), 'breadcrumbs': breadcrumbs, 'entries': entries, 'params': params })
    self._send_content(self._renderer.render('base', args), 'text/html')

  def _serve_file(self, abspath, params):
    content_type = mimetypes.guess_type(abspath)[0] or 'text/plain'
    start = int(params.get('s')[0]) if 's' in params else 0
    end = int(params.get('e')[0]) if 'e' in params else None
    with open(abspath, 'r') as infile:
      if start:
        infile.seek(start)
      content = infile.read(end - start) if end else infile.read()
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

