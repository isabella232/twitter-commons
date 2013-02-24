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

import os

from twitter.common.contextutil import  timing, get_timings
from twitter.common.dirutil import safe_mkdir, safe_rmtree

from twitter.pants import  is_scalac_plugin, get_buildroot
from twitter.pants.base.target import Target
from twitter.pants.targets.scala_library import ScalaLibrary
from twitter.pants.targets.scala_tests import ScalaTests
from twitter.pants.tasks import Task, TaskError
from twitter.pants.tasks.jvm_dependency_cache import JvmDependencyCache
from twitter.pants.tasks.nailgun_task import NailgunTask
from twitter.pants.tasks.scala.zinc_artifact import  ZincArtifactStateDiff, ZincArtifactFactory
from twitter.pants.tasks.scala.zinc_utils import ZincUtils


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
    self._artifact_factory = ZincArtifactFactory(workdir, self.context, self._zinc_utils)
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
              self._process_target_partition(vts, cp, upstream_analysis_map)
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

  def _process_target_partition(self, vts, cp, upstream_analysis_map):
    """Must run on all target partitions, not just invalid ones.

    May be invoked concurrently on independent target sets.

    Postcondition: The individual targets in vts are up-to-date, as if each were compiled individually.
    """
    artifacts = [self._artifact_factory.artifact_for_target(target) for target in vts.targets]
    merged_artifact = self._artifact_factory.merged_artifact(artifacts)
    safe_mkdir(merged_artifact.classes_dir)

    # Get anything we have from previous builds (or we pulled from the artifact cache).
    # We must do this even if we're not going to compile, because the merged output dir
    # will go on the classpath of downstream tasks. We can't put the per-target dirs
    # on the classpath because Zinc doesn't handle large numbers of upstream deps well.
    merged_artifact.merge()

    if not merged_artifact.sources:
      self.context.log.warn('Skipping scala compile for targets with no sources:\n  %s' % merged_artifact.targets)
      return

    current_state = merged_artifact.current_state()
    # Invoke the compiler if needed.
    if any([not vt.valid for vt in vts.versioned_targets]):
      old_state = current_state
      classpath = [entry for conf, entry in cp if conf in self._confs]
      self.context.log.info('Compiling targets %s' % vts.targets)
      with timing('zinc_compile'):
        if self._zinc_utils.compile(classpath, merged_artifact.sources, merged_artifact.classes_dir,
                                    merged_artifact.analysis_file, upstream_analysis_map):
          raise TaskError('Compile failed.')

      current_state = merged_artifact.current_state()

      diff = ZincArtifactStateDiff(old_state, current_state)
      print('QQQQQQQQQQQQQQQQQQ %s' % diff)

      if diff.analysis_changed:
        merged_artifact.split_classes_dir(diff)
        merged_artifact.split_analysis()
        if self._artifact_cache and self.context.options.write_to_artifact_cache:
          merged_artifact.split_portable_analysis()

      # Write the entire merged artifact, and each individual split artifact, to the artifact cache, if needed.
      self._update_artifact_cache(vts)
      for vt in vts.versioned_targets:
        self._update_artifact_cache(vt)

    # Register the products, if needed. TODO: Make sure this is safe to call concurrently.
    # In practice the GIL will make it fine, but relying on that is insanitary.
    if self.context.products.isrequired('classes'):
      with timing('update_genmap'):
        self._add_products_to_genmap(merged_artifact.classes_dir, current_state.classes_by_src, current_state.classes_by_target)

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

