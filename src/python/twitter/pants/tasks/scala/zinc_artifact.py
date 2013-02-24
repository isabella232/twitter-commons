# ==================================================================================================
# Copyright 2013 Twitter, Inc.
# --------------------------------------------------------------------------------------------------
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this work except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file, or at:
#
#  http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ==================================================================================================

__author__ = 'Benjy Weinberger'

import hashlib
import itertools
import os
import shutil
import time

from collections import defaultdict, namedtuple

from twitter.common.contextutil import temporary_dir
from twitter.common.dirutil import safe_mkdir

from twitter.pants import get_buildroot
from twitter.pants.base.target import Target
from twitter.pants.targets import resolve_target_sources
from twitter.pants.targets.scala_library import ScalaLibrary
from twitter.pants.targets.scala_tests import ScalaTests
from twitter.pants.tasks import TaskError
from twitter.pants.tasks.zinc_analysis_file import ZincAnalysisCollection


class ZincArtifactFactory(object):
  def __init__(self, workdir, context, zinc_utils):
    self._workdir = workdir
    self.context = context
    self.zinc_utils = zinc_utils

  def artifact_for_target(self, target):
    targets = [target]
    sources_by_target = {target: ZincArtifactFactory._calculate_sources(target)}
    factory = self
    return ZincArtifact(factory, targets, sources_by_target, *self._artifact_args([target]))

  def merged_artifact(self, artifacts):
    targets = list(itertools.chain.from_iterable([a.targets for a in artifacts]))
    sources_by_target = dict(itertools.chain.from_iterable([a.sources_by_target.items() for a in artifacts]))
    factory = self
    return MergedZincArtifact(artifacts, factory, targets, sources_by_target, *self._artifact_args(targets))

  def _artifact_args(self, targets):
    """Returns the artifact paths for the given target set."""
    artifact_id = Target.maybe_readable_identify(targets)
    # Each compilation must output to its own directory, so zinc can then associate those with the appropriate
    # analysis files of previous compilations.
    classes_dir = os.path.join(self._workdir, 'classes', artifact_id)
    analysis_file = os.path.join(self._workdir, 'analysis', artifact_id) + '.analysis'
    return artifact_id, classes_dir, analysis_file

  @staticmethod
  def _calculate_sources(target):
    sources = []
    srcs = [os.path.join(target.target_base, src) for src in target.sources if src.endswith('.scala')]
    sources.extend(srcs)
    if (isinstance(target, ScalaLibrary) or isinstance(target, ScalaTests)) and target.java_sources:
      sources.extend(resolve_target_sources(target.java_sources, '.java'))
    return sources


class ZincArtifact(object):
  """Locations of the files in a zinc build artifact."""
  def __init__(self, factory, targets, sources_by_target,
               artifact_id, classes_dir, analysis_file):
    self.factory = factory
    self.targets = targets
    self.sources_by_target = sources_by_target
    self.sources = list(itertools.chain.from_iterable(sources_by_target.values()))
    self.artifact_id = artifact_id
    self.classes_dir = classes_dir
    self.analysis_file = analysis_file
    self.portable_analysis_file = analysis_file + '.portable'  # The portable version of the zinc analysis file.
    self.relations_file = analysis_file + '.relations'  # The human-readable version of the zinc analysis file.

  def current_state(self):
    return ZincArtifactState(self)

  def find_all_class_files(self):
    """Returns a list of the classfiles under classes_dir, relative to that dir."""
    classes = []
    for dir, _, fs in os.walk(self.classes_dir):
      for f in fs:
        if f.endswith('.class'):
          classes.append(os.path.relpath(os.path.join(dir, f), self.classes_dir))
    return classes

  def __eq__(self, other):
    return self.artifact_id == other.artifact_id

  def __ne__(self, other):
    return self.artifact_id != other.artifact_id


class MergedZincArtifact(ZincArtifact):
  """An artifact merged from some underlying artifacts."""
  def __init__(self, underlying_artifacts, factory , targets, sources_by_target,
               artifact_id, classes_dir, analysis_file):
    ZincArtifact.__init__(self, factory, targets, sources_by_target, artifact_id, classes_dir, analysis_file)
    self.underlying_artifacts = underlying_artifacts

  def merge(self):
    self.merge_analysis()
    self.merge_classes_dir()

  def merge_analysis(self):
    if len(self.underlying_artifacts) <= 1:
      return
    with temporary_dir(cleanup=False) as tmpdir:
      artifact_analysis_files = []
      for artifact in self.underlying_artifacts:
        # Rebase a copy of the per-target analysis files to reflect the merged classes dir.
        if os.path.exists(artifact.classes_dir) and os.path.exists(artifact.analysis_file):
          analysis_file_tmp = os.path.join(tmpdir, artifact.artifact_id)
          shutil.copyfile(artifact.analysis_file, analysis_file_tmp)
          artifact_analysis_files.append(analysis_file_tmp)
          if self.factory.zinc_utils.run_zinc_rebase(analysis_file_tmp, [(artifact.classes_dir, self.classes_dir)]):
            self.factory.context.log.warn(
              'Zinc failed to rebase analysis file %s. Target may require a full rebuild.' % analysis_file_tmp)

      if self.factory.zinc_utils.run_zinc_merge(artifact_analysis_files, self.analysis_file):
        self.factory.context.log.warn(
          'zinc failed to merge analysis files %s to %s. Target may require a full rebuild.' % \
                               (':'.join(artifact_analysis_files), self.analysis_file))

  def split_analysis(self):
    self._do_split_analysis('analysis_file')

  def split_portable_analysis(self):
    self._do_split_analysis('portable_analysis_file')

  def _do_split_analysis(self, analysis_file_attr):
    if len(self.underlying_artifacts) <= 1:
      return
    # Specifies that the list of sources defines a split to the classes dir and analysis file.
    SplitInfo = namedtuple('SplitInfo', ['sources', 'dst_classes_dir', 'dst_analysis_file'])

    def _analysis(artifact):
      return getattr(artifact, analysis_file_attr)

    if len(self.underlying_artifacts) <= 1:
      return

    analysis_to_split = _analysis(self)
    if not os.path.exists(analysis_to_split):
      return

    splits = []
    for artifact in self.underlying_artifacts:
      splits.append(SplitInfo(artifact.sources, artifact.classes_dir, _analysis(artifact)))

    split_args = [(x.sources, x.dst_analysis_file) for x in splits]
    if self.factory.zinc_utils.run_zinc_split(analysis_to_split, split_args):
      raise TaskError, 'zinc failed to split analysis files %s from %s' % \
                       (':'.join([x.dst_analysis_file for x in splits]), analysis_to_split)
    for split in splits:
      if os.path.exists(split.dst_analysis_file):
        if self.factory.zinc_utils.run_zinc_rebase(split.dst_analysis_file,
                                                   [(self.classes_dir, split.dst_classes_dir)]):
          raise TaskError, 'Zinc failed to rebase analysis file %s' % split.dst_analysis_file

  def merge_classes_dir(self):
    """Merge the classes dirs from the underlying artifacts.

    May symlink instead of copying, when it's OK to do so.

    Postcondition: symlinks are of leaf packages only.
    """
    if len(self.underlying_artifacts) <= 1:
      return
    for artifact in self.underlying_artifacts:
      classnames_by_package = defaultdict(list)
      for cls in artifact.find_all_class_files():
        classnames_by_package[os.path.dirname(cls)].append(os.path.basename(cls))

      for package, classnames in classnames_by_package:
        artifact_package_dir = os.path.join(artifact.classes_dir, package)
        merged_package_dir = os.path.join(self.classes_dir, package)

        ancestor_symlink = MergedZincArtifact.find_ancestor_package_symlink(self.classes_dir, merged_package_dir)
        if not os.path.exists(merged_package_dir) and not ancestor_symlink:
          # A heuristic to prevent tons of file copying: If we're the only classes
          # in this package, we can just symlink.
          safe_mkdir(os.path.dirname(merged_package_dir))
          os.symlink(artifact_package_dir, merged_package_dir)
        else:
          # Another target already "owns" this package, so we can't use the symlink heuristic.
          # Instead, we fall back to copying. Note that the other target could have been from
          # a prior invocation of execute(), so it may not be in self.underlying_artifacts.
          if ancestor_symlink:
            # Must undo a previous symlink heuristic in this case.
            package_dir_for_some_other_target = os.readlink(ancestor_symlink)
            os.unlink(ancestor_symlink)
            shutil.copytree(package_dir_for_some_other_target, ancestor_symlink)
          safe_mkdir(merged_package_dir)
          for classname in classnames:
            src = os.path.join(artifact_package_dir, classname)
            dst = os.path.join(merged_package_dir, classname)
            # dst may already exist if we have overlapping targets. It's not a good idea
            # to have those, but until we enforce it, we must allow it here.
            if os.path.exists(src) and not os.path.exists(dst):
              os.link(src, dst)

  def split_classes_dir(self, diff):
    if len(self.underlying_artifacts) <= 1:
      return
    for artifact in self.underlying_artifacts:
      classnames_by_package = defaultdict(list)
      for cls in artifact.find_all_class_files():
        classnames_by_package[os.path.dirname(cls)].append(os.path.basename(cls))

      # TODO: Use diff to cut down on copying?
      for package, classnames in classnames_by_package:
        artifact_package_dir = os.path.join(artifact.classes_dir, package)
        merged_package_dir = os.path.join(self.classes_dir, package)

        if os.path.islink(merged_package_dir):
          linked = os.readlink(merged_package_dir)
          if linked != artifact_package_dir:
            # The output went to the wrong place. This means that this target has put classes into
            # a package previously owned exclusively by some other target.
            # First get rid of this now-invalid symlink, replacing it with a copy.
            os.unlink(merged_package_dir)
            shutil.copytree(linked, merged_package_dir)
            # Now remove our classes from the other target's dir.
            our_classnames = set(classnames)
            for f in os.listdir(linked):
              if f in our_classnames:
                os.unlink(os.path.join(linked, f))
            # We'll copy our files below.
          else:
            continue

        safe_mkdir(artifact_package_dir)
        for classname in classnames:
          src = os.path.join(merged_package_dir, classname)
          dst = os.path.join(artifact_package_dir, classname)
          shutil.copyfile(src, dst)

  @staticmethod
  def find_ancestor_package_symlink(base, dir):
    """Returns the first ancestor package of dir (including itself) under base that is a symlink."""
    while len(dir) > len(base):
      if os.path.islink(dir):
        return dir
      dir = os.path.dirname(dir)
    return None


class ZincArtifactStateDiff(object):
  def __init__(self, old_state, new_state):
    if old_state.artifact != new_state.artifact:
      raise TaskError, 'Cannot diff state of two different artifacts.'
    self.artifact = old_state.artifact
    self.new_or_changed_classes = filter(
      lambda f: os.path.getmtime(os.path.join(self.artifact.classes_dir, f)) > old_state.timestamp,
      new_state.classes)
    self.deleted_classes = old_state.classes - new_state.classes
    self.analysis_changed = old_state.analysis_fprint != new_state.analysis_fprint

  def __repr__(self):
    return 'Analysis changed: %s. New or changed classes: %d. Deleted classes: %d.' % \
           (self.analysis_changed, len(self.new_or_changed_classes), len(self.deleted_classes))


class ZincArtifactState(object):
  def __init__(self, artifact):
    self.artifact = artifact

    # Fingerprint the text version, as the binary version may vary even when the analysis is identical.
    relfile = self.artifact.relations_file
    if os.path.exists(relfile):
      self.analysis_fprint = ZincArtifactState._compute_file_fingerprint(self.artifact.relations_file)
    else:
      self.analysis_fprint = None

    self.classes_by_src = ZincArtifactState._compute_classes_by_src(self.artifact)
    self.classes_by_target = \
      ZincArtifactState._compute_classes_by_target(self.classes_by_src, self.artifact.sources_by_target)
    self.classes = set()
    # Note: It's important to use classes_by_src here, not classes_by_target, because a now-deleted src
    # won't be reflected in any target, which will screw up our computation of deleted classes.
    for classes in self.classes_by_src.values():
      classes.update(classes)

    self.timestamp = time.time()

  def artifact(self):
    return self.artifact

  @staticmethod
  def _compute_file_fingerprint(path):
    hasher = hashlib.md5()
    with open(path, 'r') as f:
      hasher.update(f.read())
    return hasher.hexdigest()

  @staticmethod
  def _compute_classes_by_src(artifact):
    # Compute target->classes and src->classes deps.
    if not os.path.exists(artifact.analysis_file):
      return {}, {}
    len_rel_classes_dir = len(artifact.classes_dir) - len(get_buildroot())
    analysis = ZincAnalysisCollection([artifact.analysis_file], stop_after='products')
    classes_by_src = {}
    for src, classes in analysis.products.items():
      classes_by_src[src] = [cls[len_rel_classes_dir:] for cls in classes]
    return classes_by_src

  @staticmethod
  def _compute_classes_by_target(classes_by_src, srcs_by_target):
    classes_by_target = defaultdict(set)
    for target, srcs in srcs_by_target.items():
      for src in srcs:
        classes_by_target[target].update(classes_by_src.get(src, []))
    return classes_by_target

