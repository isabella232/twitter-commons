import mimetypes
import os
import sys
import urlparse
import BaseHTTPServer


class FileRegionHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  """A handler that serves regions of files under a given root:

  /path/to/file?s=x&e=y serves from position x (inclusive) to position y (exclusive).
  /path/to/file?s=x serves from position x (inclusive) until the end of the file.
  /path/to/file serves the entire file.
  """
  def __init__(self, root, request, client_address, server):
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
      content_type = mimetypes.guess_type(abspath)[0] or 'text/plain'
      start = int(params.get('s')[0]) if 's' in params else 0
      end = int(params.get('e')[0]) if 'e' in params else None
      with open(abspath, 'r') as infile:
        if start:
          infile.seek(start)
        content = infile.read(end - start) if end else infile.read()
        self._send_content(content, content_type)
    except (IOError, ValueError):
      sys.stderr.write('Invalid request %s' % self.path)

  def log_message(self, format, *args):  # Silence BaseHTTPRequestHandler's logging.
    pass

class ReportingServer(object):
  def __init__(self, port, root):
    class MyHandler(FileRegionHandler):
      def __init__(self, request, client_address, server):
        FileRegionHandler.__init__(self, root, request, client_address, server)

    self._httpd = BaseHTTPServer.HTTPServer(('', port), MyHandler)
    self._httpd.timeout = 0.1  # Not the network timeout, but how often handle_request yields.

  def start(self):
    self._httpd.serve_forever()

