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
# ===================================================================================================
import itertools
import os
import shutil
from twitter.common import contextutil
from twitter.common.dirutil import safe_mkdir

from twitter.pants import has_sources, is_scalac_plugin, get_buildroot
from twitter.pants.goal.workunit import WorkUnit
from twitter.pants.targets import resolve_target_sources
from twitter.pants.targets.scala_library import ScalaLibrary
from twitter.pants.targets.scala_tests import ScalaTests
from twitter.pants.tasks import TaskError, Task
from twitter.pants.tasks.jvm_dependency_cache import JvmDependencyCache
from twitter.pants.tasks.nailgun_task import NailgunTask
from twitter.pants.reporting.reporting_utils import items_to_report_element
from twitter.pants.tasks.scala.zinc_analysis_file import ZincAnalysisCollection
from twitter.pants.tasks.scala.zinc_utils import ZincUtils


class ScalaCompile(NailgunTask):
  @classmethod
  def setup_parser(cls, option_group, args, mkflag):
    NailgunTask.setup_parser(option_group, args, mkflag)

    option_group.add_option(mkflag('warnings'), mkflag('warnings', negate=True),
                            dest='scala_compile_warnings', default=True,
                            action='callback', callback=mkflag.set_bool,
                            help='[%default] Compile scala code with all configured warnings '
                                 'enabled.')

    option_group.add_option(mkflag('plugins'), dest='plugins', default=None,
      action='append', help='Use these scalac plugins. Default is set in pants.ini.')

    option_group.add_option(mkflag('partition-size-hint'), dest='scala_compile_partition_size_hint',
      action='store', type='int', default=-1,
      help='Roughly how many source files to attempt to compile together. Set to a large number ' \
           'to compile all sources together. Set this to 0 to compile target-by-target. ' \
           'Default is set in pants.ini.')

    JvmDependencyCache.setup_parser(option_group, args, mkflag)


  def __init__(self, context):
    NailgunTask.__init__(self, context, workdir=context.config.get('scala-compile', 'nailgun_dir'))

    # Set up the zinc utils.
    color = not context.options.no_color
    self._zinc_utils = ZincUtils(context=context, nailgun_task=self, color=color)

    # The rough number of source files to build in each compiler pass.
    self._partition_size_hint = (context.options.scala_compile_partition_size_hint
                                 if context.options.scala_compile_partition_size_hint != -1
                                 else context.config.getint('scala-compile', 'partition_size_hint',
                                                            default=1000))

    # Set up dep checking if needed.
    if context.options.scala_check_missing_deps:
      JvmDependencyCache.init_product_requirements(self)

    self._opts = context.config.getlist('scala-compile', 'args')
    if context.options.scala_compile_warnings:
      self._opts.extend(context.config.getlist('scala-compile', 'warning_args'))
    else:
      self._opts.extend(context.config.getlist('scala-compile', 'no_warning_args'))

    # Various output directories.
    workdir = context.config.get('scala-compile', 'workdir')
    self._classes_dir = os.path.join(workdir, 'classes')
    self._analysis_dir = os.path.join(workdir, 'analysis')

    safe_mkdir(self._classes_dir)
    safe_mkdir(self._analysis_dir)

    self._analysis_file = os.path.join(self._analysis_dir, 'global_analysis')
    self._resources_dir = os.path.join(workdir, 'resources')

    # The ivy confs for which we're building.
    self._confs = context.config.getlist('scala-compile', 'confs')

    # The artifact cache to read from/write to.
    artifact_cache_spec = context.config.getlist('scala-compile', 'artifact_caches', default=[])
    self.setup_artifact_cache(artifact_cache_spec)

    # If we are compiling scala libraries with circular deps on java libraries we need to make sure
    # those cycle deps are present.
    self._inject_java_cycles()

    # Sources present in the last analysis that have since been deleted.
    # Generated lazily, so do not access directly. Call self._get_deleted_sources().
    self._deleted_sources = None

  def _inject_java_cycles(self):
    for scala_target in self.context.targets(lambda t: isinstance(t, ScalaLibrary)):
      for java_target in scala_target.java_sources:
        self.context.add_target(java_target)

  def product_type(self):
    return 'classes'

  def can_dry_run(self):
    return True

  def _get_deleted_sources(self):
    """Returns the list of sources present in the last analysis that have since been deleted.

    This is a global list. We have no way of associating them to individual targets.
    """
    # We compute the list lazily.
    if self._deleted_sources is None:
      with self.context.new_workunit('find-deleted-sources'):
        analysis = ZincAnalysisCollection(stop_after=ZincAnalysisCollection.PRODUCTS)
        if os.path.exists(self._analysis_file):
          analysis.add_and_parse_file(self._analysis_file, self._classes_dir)
        old_sources = analysis.products.keys()
        self._deleted_sources = filter(lambda x: not os.path.exists(x), old_sources)
    return self._deleted_sources

  def execute(self, targets):
    scala_targets = filter(lambda t: has_sources(t, '.scala'), targets)
    if not scala_targets:
      return

    write_to_artifact_cache = self._artifact_cache and self.context.options.write_to_artifact_cache

    # Get the exclusives group for the targets to compile.
    # Group guarantees that they'll be a single exclusives key for them.
    egroups = self.context.products.get_data('exclusives_groups')
    group_id = egroups.get_group_key_for_target(scala_targets[0])

    # Add resource dirs to the classpath for us and for downstream tasks.
    for conf in self._confs:
      egroups.update_compatible_classpaths(group_id, [(conf, self._resources_dir)])

    # Get the classpath generated by upstream JVM tasks (including previous calls to execute()).
    cp = egroups.get_classpath_for_group(group_id)

    # Add (only to the local copy) classpath entries necessary for our compiler plugins.
    for conf in self._confs:
      for jar in self._zinc_utils.plugin_jars():
        cp.insert(0, (conf, jar))

    # Invalidation check. Everything inside the with block must succeed for the
    # invalid targets to become valid.
    with self.invalidated(scala_targets, invalidate_dependents=True,
                          partition_size_hint=self._partition_size_hint) as invalidation_check:
      all_sources_by_target = {}
      if not self.dry_run:
        # Process partitions of invalid targets one by one.
        for vts in invalidation_check.invalid_vts_partitioned:
          sources_by_target = self._process_target_partition(vts, cp)
          all_sources_by_target.update(sources_by_target)
          vts.update()
          if write_to_artifact_cache:
            self._write_to_artifact_cache(vts, sources_by_target)

        # Check for missing dependencies, if needed.
        if invalidation_check.invalid_vts and os.path.exists(self._analysis_file):
          deps_cache = JvmDependencyCache(self.context, scala_targets, self._analysis_file, self._classes_dir)
          deps_cache.check_undeclared_dependencies()

        # Provide the target->class and source->class mappings to downstream tasks if needed.
        if self.context.products.isrequired('classes'):
          classes_by_source = self._compute_classes_by_source()
          self._add_all_products_to_genmap(all_sources_by_target, classes_by_source)

    # Update the classpath for downstream tasks.
    for conf in self._confs:
      egroups.update_compatible_classpaths(group_id, [(conf, self._classes_dir)])

  ARTIFACT_CACHE_TMPDIR = 'artifact_cache_tmpdir'

  @staticmethod
  def _analysis_for_target(analysis_dir, target):
    return os.path.join(analysis_dir, target.id + '.analysis')

  @staticmethod
  def _portable_analysis_for_target(analysis_dir, target):
    return ScalaCompile._analysis_for_target(analysis_dir, target) + '.portable'

  def _write_to_artifact_cache(self, vts, sources_by_target):
    # A temporary, but well-known, dir to munge analysis files in. Once we've written to the cache
    # we can nuke this dir. It must be well-known so we know where to find the files when we
    # retrieve them from the cache.
    analysis_dir = os.path.join(self._analysis_dir, ScalaCompile.ARTIFACT_CACHE_TMPDIR)
    safe_mkdir(analysis_dir)

    try:
      vt_by_target = dict([(vt.target, vt) for vt in vts.versioned_targets])

      # Copy the analysis file, so we can work on it without it changing under us.
      global_analysis_file_copy = os.path.join(analysis_dir, 'analysis')
      shutil.copyfile(self._analysis_file, global_analysis_file_copy)
      shutil.copyfile(self._analysis_file + '.relations', global_analysis_file_copy + '.relations')
      classes_by_source = self._compute_classes_by_source(global_analysis_file_copy)

      # This work can happen in the background, assuming analysis_dir isn't cleaned up.

      # Split the analysis into per-target files.
      splits = [(sources, ScalaCompile._analysis_for_target(analysis_dir, target))
                for target, sources in sources_by_target.items()]
      self._zinc_utils.run_zinc_split(global_analysis_file_copy, splits)

      # Relativize each split.
      # TODO: Rebase first, then split? Would require zinc changes to prevent nuking placeholders.
      with self.context.new_workunit(name='relativize-analysis', labels=[WorkUnit.MULTITOOL]):
        for target in vts.targets:
          analysis_file = ScalaCompile._analysis_for_target(analysis_dir, target)
          portable_analysis_file = ScalaCompile._portable_analysis_for_target(analysis_dir, target)
          if self._zinc_utils.relativize_analysis_file(analysis_file, portable_analysis_file):
            raise TaskError('Zinc failed to relativize analysis file: %s' % analysis_file)

      # Gather up the artifacts.
      vts_artifactfiles_pairs = []
      for target, sources in sources_by_target.items():
        artifacts = [ScalaCompile._portable_analysis_for_target(analysis_dir, target)]
        for source in sources:
          for cls in classes_by_source.get(source, []):
            artifacts.append(os.path.join(self._classes_dir, cls))
        vt = vt_by_target.get(target)
        if vt is not None:
          vts_artifactfiles_pairs.append((vt, artifacts))

      # Write to the artifact cache.
      self.update_artifact_cache(vts_artifactfiles_pairs)
    finally:
      if os.path.exists(analysis_dir):
        shutil.rmtree(analysis_dir)

  def check_artifact_cache(self, vts):
    # Special handling for scala analysis files. Class files are retrieved directly into their
    # final locations in the global classes dir.
    cached_vts, uncached_vts = Task.check_artifact_cache(self, vts)

    # The temporary, but well-known, dir the cached artifacts will retrieve analysis files into.
    # We can nuke it once we're done merging into the global analysis file.
    analysis_dir = os.path.join(self._analysis_dir, ScalaCompile.ARTIFACT_CACHE_TMPDIR)

    # Merge the cached analyses into the existing global one, and localize the whole thing.
    try:
      analyses_to_merge = []

      # Localize the cached analyses.
      # TODO: Merge first, then rebase? Would require zinc changes to prevent nuking placeholders.
      if cached_vts:
        with self.context.new_workunit(name='localize-analysis', labels=[WorkUnit.MULTITOOL]):
          for vt in cached_vts:
            for target in vt.targets:
              analysis = ScalaCompile._analysis_for_target(analysis_dir, target)
              portable_analysis = ScalaCompile._portable_analysis_for_target(analysis_dir, target)
              if os.path.exists(portable_analysis):
                if self._zinc_utils.localize_analysis_file(portable_analysis, analysis):
                  raise TaskError('Zinc failed to localize cached analysis files.')
                analyses_to_merge.append(analysis)

      if len(analyses_to_merge) > 0:
        if os.path.exists(self._analysis_file):
          analyses_to_merge.append(self._analysis_file)
        with contextutil.temporary_dir() as tmpdir:
          tmp_analysis = os.path.join(tmpdir, 'analysis')
          # Merge the cached analyses and the global one.
          if self._zinc_utils.run_zinc_merge(analyses_to_merge, tmp_analysis):
            raise TaskError('Zinc failed to merge cached analysis files.')
          shutil.copy(tmp_analysis, self._analysis_file)
          shutil.copy(tmp_analysis + '.relations', self._analysis_file + '.relations')
      return cached_vts, uncached_vts
    finally:
      if os.path.exists(analysis_dir):
        shutil.rmtree(analysis_dir)

  def _process_target_partition(self, vts, cp):
    """Needs invoking only on invalid targets.

    May be invoked concurrently on independent target sets.

    Postcondition: The individual targets in vts are up-to-date, as if each were
                   compiled individually.
    """
    def calculate_sources(target):
      """Find a target's source files."""
      sources = []
      srcs = \
        [os.path.join(target.target_base, src) for src in target.sources if src.endswith('.scala')]
      sources.extend(srcs)
      if (isinstance(target, ScalaLibrary) or isinstance(target, ScalaTests)) and target.java_sources:
        sources.extend(resolve_target_sources(target.java_sources, '.java'))
      return sources

    sources_by_target = dict([(t, calculate_sources(t)) for t in vts.targets])
    sources = list(itertools.chain.from_iterable(sources_by_target.values()))

    if not sources:
      self.context.log.warn('Skipping scala compile for targets with no sources:\n  %s' % vts.targets)
    else:
      # Do some reporting.
      self.context.log.info(
        'Operating on a partition containing ',
        items_to_report_element(vts.cache_key.sources, 'source'),
        ' in ',
        items_to_report_element([t.address.reference() for t in vts.targets], 'target'), '.')
      classpath = [entry for conf, entry in cp if conf in self._confs]
      deleted_sources = self._get_deleted_sources()
      with self.context.new_workunit('compile'):
        # Zinc may delete classfiles, then later exit on a compilation error. Then if the
        # change triggering the error is reverted, we won't rebuild to restore the missing
        # classfiles. So we force-invalidate here, to be on the safe side.
        vts.force_invalidate()   # TODO: Still need this?
        if self._zinc_utils.compile(classpath, sources + deleted_sources,
                                    self._classes_dir,self._analysis_file, {}):
          raise TaskError('Compile failed.')
    return sources_by_target

  def _compute_classes_by_source(self, analysis_file=None):
    """Compute src->classes."""
    if analysis_file is None:
      analysis_file = self._analysis_file

    if not os.path.exists(analysis_file):
      return {}
    len_rel_classes_dir = len(self._classes_dir) - len(get_buildroot())
    analysis = ZincAnalysisCollection(stop_after=ZincAnalysisCollection.PRODUCTS)
    analysis.add_and_parse_file(analysis_file, self._classes_dir)
    classes_by_src = {}
    for src, classes in analysis.products.items():
      classes_by_src[src] = [cls[len_rel_classes_dir:] for cls in classes]
    return classes_by_src

  def _add_all_products_to_genmap(self, sources_by_target, classes_by_source):
    # Map generated classes to the owning targets and sources.
    genmap = self.context.products.get('classes')
    for target, sources in sources_by_target.items():
      for source in sources:
        classes = classes_by_source.get(source, [])
        relsrc = os.path.relpath(source, target.target_base)
        genmap.add(relsrc, self._classes_dir, classes)
        genmap.add(target, self._classes_dir, classes)

      # TODO(John Sirois): Map target.resources in the same way
      # Create and Map scala plugin info files to the owning targets.
      if is_scalac_plugin(target) and target.classname:
        basedir, plugin_info_file = self._zinc_utils.write_plugin_info(self._resources_dir, target)
        genmap.add(target, basedir, [plugin_info_file])
