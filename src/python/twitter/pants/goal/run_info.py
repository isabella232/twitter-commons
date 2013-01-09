
import os
import re

from twitter.common.dirutil import safe_mkdir_for


class RunInfo(object):
  """A little plaintext file containing very basic info about a pants run.

  Can only be appended to, never edited."""
  def __init__(self, info_file):
    self._info_file = info_file
    safe_mkdir_for(self._info_file)
    self._info = {}
    if os.path.exists(self._info_file):
      with open(self._info_file, 'r') as infile:
        info = infile.read()
      for m in re.finditer("""^([^:]+):(.*)$""", info, re.MULTILINE):
        self._info[m.group(1).strip()] = m.group(2).strip()

  def get_info(self, key):
    return self._info.get(key, None)

  def __getattr__(self, key):
    ret = self.get_info(key)
    if ret is None:
      raise KeyError, key
    return ret

  def get_as_dict(self):
    return self._info.copy()

  def add_info(self, key, val):
    self.add_infos([(key, val)])

  def add_infos(self, keyvals):
    with open(self._info_file, 'a') as outfile:
      for key, val in keyvals:
        if ':' in key:
          raise Exception, 'info key must not contain a colon'
        outfile.write('%s: %s\n' % (key, val))
        self._info[key] = val
