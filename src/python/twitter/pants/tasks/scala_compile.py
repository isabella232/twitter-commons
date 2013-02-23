# ==================================================================================================
# Copyright 2011 Twitter, Inc.
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
import os
import shutil
import time

from collections import defaultdict, namedtuple

from twitter.common.contextutil import  temporary_dir, timing, get_timings
from twitter.common.dirutil import safe_mkdir, safe_rmtree
from twitter.common.util import find_common_path_prefix

from twitter.pants import  is_scalac_plugin, get_buildroot
from twitter.pants.base.target import Target
from twitter.pants.targets.scala_library import ScalaLibrary
from twitter.pants.targets.scala_tests import ScalaTests
from twitter.pants.targets import resolve_target_sources
from twitter.pants.tasks import Task, TaskError
from twitter.pants.tasks.jvm_dependency_cache import JvmDependencyCache
from twitter.pants.tasks.zinc_analysis_file import ZincAnalysisCollection
from twitter.pants.tasks.nailgun_task import NailgunTask
from twitter.pants.tasks.zinc_utils import ZincUtils


# There are two versions of the zinc analysis file: The one zinc creates on compilation, which
# contains full paths and is therefore not portable, and the portable version, that we create by rebasing
# the full path prefixes to placeholders. We refer to this as "relativizing" the analysis file.
# The inverse, replacing placeholders with full path prefixes so we can use the file again when compiling,
# is referred to as "localizing" the analysis file.
#
# This is necessary only when using the artifact cache: We must relativize before uploading to the cache,
# and localize after pulling from the cache.

def _portable(analysis_file):
  """Returns the path to the portable version of the zinc analysis file."""
  return analysis_file + '.portable'

def _relations(analysis_file):
  """Returns the path to the human-readable text version of the zinc analysis file."""
  return analysis_file + '.relations'


class ScalaCompile(NailgunTask):
  @staticmethod
  def _has_scala_sources(target):
    return isinstance(target, ScalaLibrary) or isinstance(target, ScalaTests)

  @classmethod
  def setup_parser(cls, option_group, args, mkflag):
    NailgunTask.setup_parser(option_group, args, mkflag)

    option_group.add_option(mkflag("warnings"), mkflag("warnings", negate=True),
                            dest="scala_compile_warnings", default=True,
                            action="callback", callback=mkflag.set_bool,
                            help="[%default] Compile scala code with all configured warnings "
                                 "enabled.")

    option_group.add_option(mkflag("plugins"), dest="plugins", default=None,
      action="append", help="Use these scalac plugins. Default is set in pants.ini.")

    option_group.add_option(mkflag("partition-size-hint"), dest="scala_compile_partition_size_hint",
      action="store", type="int", default=-1,
      help="Roughly how many source files to attempt to compile together. Set to a large number to compile " \
           "all sources together. Set this to 0 to compile target-by-target. Default is set in pants.ini.")

    option_group.add_option(mkflag("color"), mkflag("color", negate=True),
                            dest="scala_compile_color",
                            action="callback", callback=mkflag.set_bool,
                            help="[True] Enable color in logging.")
    JvmDependencyCache.setup_parser(option_group, args, mkflag)


  def __init__(self, context, workdir=None):
    NailgunTask.__init__(self, context, workdir=context.config.get('scala-compile', 'nailgun_dir'))

    # Set up the zinc utils.
    color = context.options.scala_compile_color or \
            context.config.getbool('scala-compile', 'color', default=True)

    self._zinc_utils = ZincUtils(context=context, java_runner=self.runjava, color=color)

    # The rough number of source files to build in each compiler pass.
    self._partition_size_hint = \
      context.options.scala_compile_partition_size_hint \
      if context.options.scala_compile_partition_size_hint != -1 else \
      context.config.getint('scala-compile', 'partition_size_hint')

    # Set up dep checking if needed.
    if context.options.scala_check_missing_deps:
      JvmDependencyCache.init_product_requirements(self)

    # Various output directories.
    self._buildroot = get_buildroot()
    workdir = context.config.get('scala-compile', 'workdir') if workdir is None else workdir
    self._resources_dir = os.path.join(workdir, 'resources')
    self._classes_dir_base = os.path.join(workdir, 'classes')
    self._analysis_files_base = os.path.join(workdir, 'analysis_cache')

    # The ivy confs for which we're building.
    self._confs = context.config.getlist('scala-compile', 'confs')

    # The artifact cache to read from/write to.
    artifact_cache_spec = context.config.getlist('scala-compile', 'artifact_caches')
    self.setup_artifact_cache(artifact_cache_spec)

  def product_type(self):
    return 'classes'

  def can_dry_run(self):
    return True

  def _output_paths(self, targets):
    """Returns the full paths to the classes dir and analysis file for the given target set."""
    compilation_id = Target.maybe_readable_identify(targets)
    # Each compilation must output to its own directory, so zinc can then associate those with the appropriate
    # analysis files of previous compilations.
    classes_dir = os.path.join(self._classes_dir_base, compilation_id)
    analysis_file = os.path.join(self._analysis_files_base, compilation_id) + '.analysis'
    return classes_dir, analysis_file

  def execute(self, targets):
    scala_targets = filter(ScalaCompile._has_scala_sources, targets)
    if not scala_targets:
      return

    safe_mkdir(self._classes_dir_base)
    safe_mkdir(self._analysis_files_base)

    # Get the classpath generated by upstream JVM tasks (including previous calls to this execute()).
    with self.context.state('classpath', []) as cp:
      self._add_globally_required_classpath_entries(cp)
      with self.context.state('upstream_analysis_map', {}) as upstream_analysis_map:
        with self.invalidated(scala_targets, invalidate_dependents=True,
                              partition_size_hint=self._partition_size_hint) as invalidation_check:
          # Process partitions one by one.
          for vts in invalidation_check.all_vts_partitioned:
            if not self.dry_run:
              with timing('process_targets'):
                self._process_targets(vts, cp, upstream_analysis_map)
              vts.update()
              classes_dir, analysis_file = self._output_paths(vts.targets)
              # Note that we add the pre-split classes_dir to the upstream.
              # This is because zinc doesn't handle many upstream dirs well.
              if os.path.exists(classes_dir):
                for conf in self._confs:
                  cp.append((conf, classes_dir))
                if os.path.exists(analysis_file):
                  upstream_analysis_map[classes_dir] = analysis_file

    # Check for missing dependencies.
    all_analysis_files = set()
    for target in scala_targets:
      _, analysis_file = self._output_paths([target])
      if os.path.exists(analysis_file):
        all_analysis_files.add(analysis_file)
    deps_cache = JvmDependencyCache(self.context, scala_targets, all_analysis_files)
    deps_cache.check_undeclared_dependencies()

    print(get_timings())

  def _add_globally_required_classpath_entries(self, cp):
    # Add classpath entries necessary both for our compiler calls and for downstream JVM tasks.
    for conf in self._confs:
      cp.insert(0, (conf, self._resources_dir))
      for jar in self._zinc_utils.plugin_jars():
        cp.insert(0, (conf, jar))

  def _localize_portable_artifact_files(self, vts):
    # Localize the analysis files we read from the artifact cache.
    for vt in vts:
      _, analysis_file = self._output_paths(vt.targets)
      if self._zinc_utils.localize_analysis_file(_portable(analysis_file), analysis_file):
        self.context.log.warn('Zinc failed to localize analysis file: %s. '\
                              'Incremental rebuild of that target may not be possible.' % analysis_file)

  def check_artifact_cache(self, vts):
    cached_vts = Task.check_artifact_cache(self, vts)
    self._localize_portable_artifact_files(cached_vts)
    return cached_vts

  def _process_targets(self, vts, cp, upstream_analysis_map):
    """Must run on all target partitions, not just invalid ones.

    May be invoked concurrently on independent target sets.

    Postcondition: The individual targets in versioned_target_set are up-to-date, as if each
                   were compiled individually.
    """
    classes_dir, analysis_file = self._output_paths(vts.targets)
    safe_mkdir(classes_dir)

    # Get anything we have from previous builds (or we pulled from the artifact cache).
    # We must do this even if we're not going to compile, because the merged output dir
    # will go on the classpath of downstream tasks. We can't put the per-target dirs
    # on the classpath because Zinc doesn't handle large numbers of upstream deps well.
    with timing('merge_artifact'):
      self._merge_artifact(vts.targets)

    # Compute the sources we need to compile.
    srcs_by_target = ScalaCompile._calculate_sources(vts.targets)

    srcs = reduce(lambda all, sources: all.union(sources), srcs_by_target.values()) if srcs_by_target else None
    if not srcs:
      self.context.log.warn('Skipping scala compile for targets with no sources:\n  %s' %
                           '\n  '.join(str(t) for t in srcs_by_target.keys()))
      return

    classes_by_src, classes_by_target = None, None

    # Invoke the compiler if needed.
    if any([not vt.valid for vt in vts.versioned_targets]):
      # Fingerprint the text version, as the binary version may vary even when the analysis is identical.
      with timing('capture_old_state'):
        if os.path.exists(analysis_file):
          old_analysis_fprint = ScalaCompile._compute_file_fingerprint(_relations(analysis_file))
          #old_classes = set(ScalaCompile._find_all_class_files(classes_dir))
          _, old_classes_by_target = self._compute_classfile_maps(srcs_by_target, vts.targets)
          old_classes = set()
          for classes in old_classes_by_target.values():
            old_classes.update(classes)
        else:
          old_analysis_fprint = None
          old_classes = set()
        old_timestamp = time.time()

      classpath = [entry for conf, entry in cp if conf in self._confs]
      self.context.log.info('Compiling targets %s' % vts.targets)
      with timing('zinc_compile'):
        if self._zinc_utils.compile(classpath, srcs, classes_dir, analysis_file, upstream_analysis_map):
          raise TaskError('Compile failed.')

      classes_by_src, classes_by_target = self._compute_classfile_maps(srcs_by_target, vts.targets)

      with timing('compute_new_state'):
        new_analysis_fprint = ScalaCompile._compute_file_fingerprint(_relations(analysis_file))
        #current_classes = set(ScalaCompile._find_all_class_files(classes_dir))
        current_classes = set()
        for classes in classes_by_target.values():
          current_classes.update(classes)

      with timing('filter_classfiles'):
        new_or_changed_classes = \
        filter(lambda f: os.path.getmtime(os.path.join(classes_dir, f)) > old_timestamp, current_classes)
      deleted_classes = old_classes - current_classes

      print('QQQQQQQQQQQQQQQQQQ new: %d  deleted: %s' % (len(new_or_changed_classes), len(deleted_classes)))

      split_analysis = new_analysis_fprint != old_analysis_fprint
      split_portable_analysis = self._artifact_cache and self.context.options.write_to_artifact_cache
      with timing('split_artifact'):
        # Split the artifact we just compiled into per-target artifacts.
        self._split_artifact(srcs_by_target, classes_by_target,
                             new_or_changed_classes, deleted_classes,
                             split_analysis,
                             split_portable_analysis,
                             vts.targets)

      # Write the entire merged artifact, and each individual split artifact, to the artifact cache, if needed.
      self._update_artifact_cache(vts)
      for vt in vts.versioned_targets:
        self._update_artifact_cache(vt)

    # Register the products, if needed. TODO: Make sure this is safe to call concurrently.
    # In practice the GIL will make it fine, but relying on that is insanitary.
    if self.context.products.isrequired('classes'):
      with timing('update_genmap'):
        if not classes_by_src or not classes_by_target:
          classes_by_src, classes_by_target = self._compute_classfile_maps(srcs_by_target, vts.targets)
        self._add_products_to_genmap(classes_dir, classes_by_src, classes_by_target)

  def _add_products_to_genmap(self, classes_dir, classes_by_src, classes_by_target):
    """Must be called on all targets, whether they needed compilation or not."""
    genmap = self.context.products.get('classes')
    for source, classes in classes_by_src:
      genmap.add(source, classes_dir, classes)
    for target, classes in classes_by_target:
      genmap.add(target, classes_dir, classes)
      # TODO(John Sirois): Map target.resources in the same way
      # Create and Map scala plugin info files to the owning targets.
      if is_scalac_plugin(target) and target.classname:
        basedir, plugin_info_file = self._zinc_utils.write_plugin_info(self._resources_dir, target)
        genmap.add(target, basedir, [plugin_info_file])

  def _compute_classfile_maps(self, srcs_by_target, targets):
    # Compute target->classes and src->classes deps from the zinc analysis.
    classes_dir, analysis_file = self._output_paths(targets)
    len_rel_classes_dir = len(classes_dir) - len(self._buildroot)
    with timing('compute_maps'):
      with timing('parse_analysis_file'):
        analysis = ZincAnalysisCollection([analysis_file], stop_after='products')
      classes_by_src = {}
      for src, classes in analysis.products.items():
        classes_by_src[src] = [cls[len_rel_classes_dir:] for cls in classes]
      classes_by_target = defaultdict(set)
      for target, srcs in srcs_by_target.items():
        for src in srcs:
          classes_by_target[target].update(classes_by_src.get(src, []))
      return classes_by_src, classes_by_target

  def _update_artifact_cache(self, vt):
    classes_dir, analysis_file = self._output_paths(vt.targets)
    portable_analysis_file = _portable(analysis_file)
    if self._artifact_cache and self.context.options.write_to_artifact_cache:
      # Relativize the analysis.
      # TODO: Relativize before splitting? This will require changes to Zinc, which currently
      # eliminates paths it doesn't recognize (including our placeholders) when splitting.
      if os.path.exists(analysis_file) and \
         self._zinc_utils.relativize_analysis_file(analysis_file, portable_analysis_file):
        raise TaskError('Zinc failed to relativize analysis file: %s' % analysis_file)
        # Write the per-target artifacts to the cache.
      artifacts = [classes_dir, portable_analysis_file]
      self.update_artifact_cache(vt, artifacts)
    else:
      safe_rmtree(portable_analysis_file)  # Don't leave cruft lying around.

  def _merge_artifact(self, targets):
    """Merges artifacts representing the individual targets into one artifact.
    Creates an output classes dir and analysis file for the combined artifact.
    Note that the merged artifact may be incomplete (e.g., if we have no previous artifacts for some of the
    individual targets). That's OK: We run this right before we invoke zinc, which will fill in what's missing.
    This method is not required for correctness, only for efficiency: it can prevent zinc from doing superfluous work.

    NOTE: This method is reentrant.
    """
    if len(targets) <= 1:
      return  # Nothing to do.

    dst_classes_dir, dst_analysis_file = self._output_paths(targets)

    # If we have all three then we have a valid merged artifact from a previous compile.
    # In some unlikely corner cases (e.g., when we previously built the constituent targets in other groupings)
    # the artifact might be older than one we could create by merging again, and in those cases zinc might end up
    # doing more work than it strictly has to. But it's worth sacrificing a little performance in those corner
    # cases so we don't do a ton of repetitive merging in the common case.
    if os.path.exists(dst_classes_dir) and os.path.exists(dst_analysis_file):
      return

    with temporary_dir(cleanup=False) as tmpdir:
      safe_rmtree(dst_classes_dir)
      safe_mkdir(dst_classes_dir)
      src_analysis_files = []

      for target in targets:
        src_classes_dir, src_analysis_file = self._output_paths([target])
        classes = ScalaCompile._find_all_class_files(src_classes_dir)

        # The package directories for all classes in this target.
        package_dirs = set([os.path.dirname(cls) for cls in classes])
        common_package_dir = find_common_path_prefix(package_dirs)

        src_package_dir = os.path.join(src_classes_dir, common_package_dir)
        dst_package_dir = os.path.join(dst_classes_dir, common_package_dir)

        # Returns the first ancestor package of dir (including itself) that is a symlink.
        def find_ancestor_package_symlink(dir):
          if os.path.islink(dir):
            return dir
          parent = os.path.dirname(dir)
          return None if parent == dst_classes_dir else find_ancestor_package_symlink(parent)

        ancestor_symlink = find_ancestor_package_symlink(dst_package_dir)

        if os.path.exists(dst_package_dir) or ancestor_symlink:
          # Another target already has classes under this package, or some parent used
          # the symlink heuristic, so we can't use it. Instead, we fall back to using hard copies.
          if ancestor_symlink:
            # Can't use the symlink heuristic in this case.
            # Make a hard copy of the previous target's classes instead.
            previous_src_dir = os.readlink(ancestor_symlink)
            os.unlink(ancestor_symlink)
            shutil.copytree(previous_src_dir, ancestor_symlink)
          for dir in package_dirs:
            safe_mkdir(os.path.join(dst_classes_dir, dir))
          with timing('merge_artifact_copy_classes'):
            for cls in classes:
              src = os.path.join(src_classes_dir, cls)
              dst = os.path.join(dst_classes_dir, cls)
              # src may not exist if we aborted a build in the middle. That's OK: zinc will notice that
              # it's missing and rebuild it.
              # dst may already exist if we have overlapping targets. It's not a good idea
              # to have those, but until we enforce it, we must allow it here.
              if os.path.exists(src) and not os.path.exists(dst):
                os.link(src, dst)
        else:
          # A heuristic to prevent tons of file copying. As long as we're the only target
          # with classes in this package, we can just symlink.
          safe_mkdir(os.path.dirname(dst_package_dir))
          os.symlink(src_package_dir, dst_package_dir)

        # Rebase a copy of the per-target analysis files to reflect the merged classes dir.
        if os.path.exists(src_classes_dir) and os.path.exists(src_analysis_file):
          src_analysis_file_tmp = \
          os.path.join(tmpdir, os.path.relpath(src_analysis_file, self._analysis_files_base))
          shutil.copyfile(src_analysis_file, src_analysis_file_tmp)
          src_analysis_files.append(src_analysis_file_tmp)
          with timing('merge_artifact_zinc_rebase'):
            if self._zinc_utils.run_zinc_rebase(src_analysis_file_tmp, [(src_classes_dir, dst_classes_dir)]):
              self.context.log.warn('In merge_artifact: zinc failed to rebase analysis file %s. '\
                                    'Target may require a full rebuild.' % \
                                    src_analysis_file_tmp)

      with timing('merge_artifact_zinc_merge'):
        if self._zinc_utils.run_zinc_merge(src_analysis_files, dst_analysis_file):
          self.context.log.warn('zinc failed to merge analysis files %s to %s. ' \
                                'Target may require a full rebuild.' % \
                                (':'.join(src_analysis_files), dst_analysis_file))

  def _split_artifact(self, srcs_by_target, classes_by_target,
                      new_or_changed_classes, deleted_classes,
                      split_analysis, split_portable_analysis,
                      targets):
    """Splits an artifact representing several targets into target-by-target artifacts.
    Creates an output classes dir and an analysis file for each target.
    Note that it's not OK to create incomplete artifacts here: this is run *after* a zinc invocation,
    and the expectation is that the result is complete.

    NOTE: This method is reentrant.
    """
    if len(targets) <= 1:
      return
    src_classes_dir, src_analysis_file = self._output_paths(targets)

    # Specifies that the list of sources defines a split to the classes dir and analysis file.
    SplitInfo = namedtuple('SplitInfo', ['sources', 'dst_classes_dir', 'dst_analysis_file'])

    analysis_splits = []  # List of SplitInfos.
    portable_analysis_splits = []  # The same, for the portable version of the analysis cache.

    for target in targets:
      dst_classes_dir, dst_analysis_file = self._output_paths([target])
      # Prepare the split arguments.
      sources = srcs_by_target[target]
      analysis_splits.append(SplitInfo(sources, dst_classes_dir, dst_analysis_file))
      portable_analysis_splits.append(SplitInfo(sources, dst_classes_dir, _portable(dst_analysis_file)))

      # Copy the class files.
      classes = classes_by_target[target]

      # The package directories for all classes in this target.
      package_dirs = set([os.path.dirname(cls) for cls in classes])
      common_package_dir = find_common_path_prefix(package_dirs)

      src_package_dir = os.path.join(src_classes_dir, common_package_dir)
      copy_classes = not os.path.islink(src_package_dir)

      # If we used the symlink heuristic we don't need to update dst_classes_dir.
      if copy_classes:
        for dir in package_dirs:
          safe_mkdir(os.path.join(dst_classes_dir, dir))
        with timing('split_artifact_copy_classes'):
          for cls in new_or_changed_classes:
            dst = os.path.join(dst_classes_dir, cls)
            with timing('split_artifact_copy_classes_shutil'):
              shutil.copyfile(os.path.join(src_classes_dir, cls), dst)
          for cls in deleted_classes:
            dst = os.path.join(dst_classes_dir, cls)
            if os.path.exists(dst):
              os.unlink(dst)

    if split_analysis:
      def do_split(src_analysis_file, splits):
        if os.path.exists(src_analysis_file):
          if self._zinc_utils.run_zinc_split(src_analysis_file, [(x.sources, x.dst_analysis_file) for x in splits]):
            raise TaskError, 'zinc failed to split analysis files %s from %s' %\
                             (':'.join([x.dst_analysis_file for x in splits]), src_analysis_file)
          with timing('split_artifact_zinc_rebase'):
            for split in splits:
              if os.path.exists(split.dst_analysis_file):
                if self._zinc_utils.run_zinc_rebase(split.dst_analysis_file,
                                                    [(src_classes_dir, split.dst_classes_dir)]):
                  raise TaskError, \
                    'In split_artifact: zinc failed to rebase analysis file %s' % split.dst_analysis_file

      # Now rebase the newly created analysis file(s) to reflect the split classes dirs.
      with timing('split_artifact_zinc_split'):
        do_split(src_analysis_file, analysis_splits)
        if split_portable_analysis:
          do_split(_portable(src_analysis_file), portable_analysis_splits)

  @staticmethod
  def _find_all_class_files(classes_dir):
    """Returns a list of the classfiles under classes_dir, relative to that dir."""
    with timing('do_find_all_classfiles'):
      classes = []
      for dir, _, fs in os.walk(classes_dir):
        for f in fs:
          if f.endswith('.class'):
            classes.append(os.path.relpath(os.path.join(dir, f), classes_dir))
      return classes

  @staticmethod
  def _compute_file_fingerprint(path):
    with timing('fprint'):
      hasher = hashlib.md5()
      with open(path, 'r') as f:
        hasher.update(f.read())
      return hasher.hexdigest()

  @staticmethod
  def _calculate_sources(targets):
    sources = defaultdict(set)
    def collect_sources(target):
      src = (os.path.join(target.target_base, source)
             for source in target.sources if source.endswith('.scala'))
      if src:
        sources[target].update(src)

        if (isinstance(target, ScalaLibrary) or isinstance(target, ScalaTests)) and (
            target.java_sources):
          sources[target].update(resolve_target_sources(target.java_sources, '.java'))

    for target in targets:
      collect_sources(target)
    return sources
