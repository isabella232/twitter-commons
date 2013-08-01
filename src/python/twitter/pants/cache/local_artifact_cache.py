import os
import shutil
from twitter.common.dirutil import safe_mkdir, safe_rmtree, safe_mkdir_for
from twitter.pants.cache.artifact import DirectoryArtifact
from twitter.pants.cache.artifact_cache import ArtifactCache


class LocalArtifactCache(ArtifactCache):
  """An artifact cache that stores the artifacts in local files."""
  def __init__(self, log, artifact_root, cache_root, copy_fn=None):
    """
    cache_root: The locally cached files are stored under this directory.
    copy_fn: An optional function with the signature copy_fn(absolute_src_path, relative_dst_path) that
        will copy cached files into the desired destination. If unspecified, a simple file copy is used.
    """
    ArtifactCache.__init__(self, log, artifact_root)
    self._cache_root = os.path.expanduser(cache_root)

    def copy(src, rel_dst):
      dst = os.path.join(self.artifact_root, rel_dst)
      safe_mkdir_for(dst)
      shutil.copy(src, dst)

    self._copy_fn = copy_fn or copy
    safe_mkdir(self._cache_root)

  def try_insert(self, cache_key, paths):
    cache_dir = self._cache_dir_for_key(cache_key)
    # Write to a temporary name, and move it atomically, so if we
    # crash in the middle we don't leave an incomplete artifact.
    cache_dir_tmp = cache_dir + '.tmp'
    safe_rmtree(cache_dir_tmp)
    artifact = DirectoryArtifact(self.artifact_root, cache_dir_tmp, self._copy_fn)
    artifact.collect(paths)
    # Note: Race condition here if multiple pants runs (in different workspaces)
    # try to write the same thing at the same time. However since rename is atomic,
    # this should not result in corruption.
    safe_rmtree(cache_dir)
    os.rename(cache_dir_tmp, cache_dir)

  def has(self, cache_key):
    return os.path.isdir(self._cache_dir_for_key(cache_key))

  def use_cached_files(self, cache_key):
    cache_dir = self._cache_dir_for_key(cache_key)
    if os.path.exists(cache_dir):
      artifact = DirectoryArtifact(self.artifact_root, cache_dir, self._copy_fn)
      artifact.extract()
      return artifact
    else:
      return None

  def get_artifact_for_key(self, cache_key):
    cache_dir = self._cache_dir_for_key(cache_key)
    return DirectoryArtifact(self.artifact_root, cache_dir, self._copy_fn)

  def delete(self, cache_key):
    safe_rmtree(self._cache_dir_for_key(cache_key))

  def _cache_dir_for_key(self, cache_key):
    # Note: it's important to use the id as well as the hash, because two different targets
    # may have the same hash if both have no sources, but we may still want to differentiate them.
    return os.path.join(self._cache_root, cache_key.id, cache_key.hash)
