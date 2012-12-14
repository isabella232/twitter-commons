import mimetypes
import os
import pystache
import sys
import urlparse
import BaseHTTPServer

from twitter.pants.reporting.renderer import Renderer


class FileRegionHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  """A handler that serves regions of files under a given root:

  /path/to/file?s=x&e=y serves from position x (inclusive) to position y (exclusive).
  /path/to/file?s=x serves from position x (inclusive) until the end of the file.
  /path/to/file serves the entire file.

  templates are a map from template name to template text, for when we need templates (e.g.,
  to render a directory listing nicely).
  """
  def __init__(self, renderer, root, request, client_address, server):
    self._renderer = renderer
    self._root = root
    BaseHTTPServer.BaseHTTPRequestHandler.__init__(self, request, client_address, server)

  def _send_content(self, content, content_type):
    self.send_response(200)
    self.send_header('Content-Type', content_type)
    self.send_header('Content-Length', str(len(content)))
    self.end_headers()
    self.wfile.write(content)

  def do_GET(self):
    try:
      (_, _, path, query, _) = urlparse.urlsplit(self.path)
      params = urlparse.parse_qs(query)
      abspath = os.path.normpath(os.path.join(self._root, path[1:]))
      if not abspath.startswith(self._root):
        raise ValueError  # Prevent using .. to get files from anywhere other than root.
      if os.path.isdir(abspath):
        self._serve_dir(abspath, params)
      elif os.path.isfile(abspath):
        self._serve_file(abspath, params)
    except (IOError, ValueError):
      sys.stderr.write('Invalid request %s' % self.path)

  def _serve_dir(self, abspath, params):
    link_base = '/' + os.path.relpath(abspath, self._root)
    entries = [ {'link': os.path.join(link_base, e), 'name': e} for e in os.listdir(abspath)]
    args = self._default_template_args('dir')
    args.update({ 'entries': entries, 'params': params })
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
  def __init__(self, port, template_dir, root):
    renderer = Renderer(template_dir, require=['base'])

    class MyHandler(FileRegionHandler):
      def __init__(self, request, client_address, server):
        FileRegionHandler.__init__(self, renderer, root, request, client_address, server)

    self._httpd = BaseHTTPServer.HTTPServer(('', port), MyHandler)
    self._httpd.timeout = 0.1  # Not the network timeout, but how often handle_request yields.

  def start(self):
    self._httpd.serve_forever()

