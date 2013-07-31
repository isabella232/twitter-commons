from contextlib import contextmanager
import os
import shutil
from twitter.common.contextutil import temporary_file_path, open_tar
from twitter.common.dirutil import safe_mkdir_for, safe_mkdir


class ArtifactError(Exception):
  pass


class Artifact(object):
  """Represents a set of files in an artifact."""
  def __init__(self, artifact_root):
    # All files must be under this root.
    self._artifact_root = artifact_root

    # The files known to be in this artifact, relative to artifact_root.
    self._relpaths = set()

  def collect(self, paths):
    """Collect the paths (which must be under artifact root) into this artifact."""
    raise NotImplementedError()

  def extract(self):
    """Extract the files in this artifact to their locations under artifact root."""
    raise NotImplementedError()


class DirectoryArtifact(Artifact):
  """An artifact stored as loose files under a directory."""
  def __init__(self, artifact_root, directory, copy_fn=None):
    Artifact.__init__(self, artifact_root)
    self._directory = directory

    def copy(src, rel_dst):
      dst = os.path.join(self._artifact_root, rel_dst)
      safe_mkdir_for(dst)
      shutil.copy(src, dst)
    self._copy_fn = copy_fn or copy

  def collect(self, paths):
    for path in paths or ():
      relpath = os.path.relpath(path, self._artifact_root)
      dst = os.path.join(self._directory, relpath)
      safe_mkdir(os.path.dirname(dst))
      if os.path.isdir(path):
        shutil.copytree(path, dst)
      else:
        shutil.copy(path, dst)
      self._relpaths.add(path)

  def extract(self):
    if not os.path.exists(self._directory):
      return False
    for dir_name, _, filenames in os.walk(self._directory):
      for filename in filenames:
        filename = os.path.join(dir_name, filename)
        relpath = os.path.relpath(filename, self._directory)
        self._copy_fn(filename, relpath)
        self._relpaths.add(relpath)
    return True


class TarballArtifact(Artifact):
  """An artifact stored in a tarball."""
  def __init__(self, artifact_root, tarfile, compress):
    Artifact.__init__(self, artifact_root)
    self._tarfile = tarfile
    self._compress = compress

  def collect(self, paths):
    # In our tests, gzip is slightly less compressive than bzip2 on .class files,
    # but decompression times are much faster.
    mode = 'w:gz' if self._compress else 'w'
    with open_tar(self._tarfile, mode, dereference=True) as tarout:
      for path in paths:
        # Adds dirs recursively.
        relpath = os.path.relpath(path, self._artifact_root)
        tarout.add(path, relpath)
        self._relpaths.add(relpath)

  def extract(self):
    with open_tar(self._tarfile, 'r') as tarin:
      tarin.extractall(self._artifact_root)
      self._relpaths.update(tarin.getnames())
