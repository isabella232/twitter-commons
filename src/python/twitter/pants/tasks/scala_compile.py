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
from twitter.common.dirutil import safe_mkdir

from twitter.pants import has_sources, is_scalac_plugin, get_buildroot
from twitter.pants.base import Target
from twitter.pants.targets import resolve_target_sources
from twitter.pants.targets.scala_library import ScalaLibrary
from twitter.pants.targets.scala_tests import ScalaTests
from twitter.pants.tasks import TaskError
from twitter.pants.tasks.jvm_dependency_cache import JvmDependencyCache
from twitter.pants.tasks.nailgun_task import NailgunTask
from twitter.pants.reporting.reporting_utils import items_to_report_element
from twitter.pants.tasks.scala.zinc_analysis_file import ZincAnalysisCollection
from twitter.pants.tasks.scala.zinc_utils import ZincUtils


def _is_scala(target):
  return has_sources(target, '.scala')


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
        print('RRRRRRRR %s' % self._deleted_sources)
    return self._deleted_sources

  def execute(self, targets):
    scala_targets = filter(_is_scala, targets)
    if not scala_targets:
      return

    write_to_artifact_cache = self._artifact_cache and self.context.options.write_to_artifact_cache

    # Get the exclusives group for the targets to compile.
    # Group guarantees that they'll be a single exclusives key for them.
    egroups = self.context.products.get_data('exclusives_groups')
    exclusives_key = egroups.get_group_key_for_target(targets[0])

    # Get the classpath generated by upstream JVM tasks (including previous calls to execute()).
    cp = egroups.get_classpath_for_group(exclusives_key)

    # Add classpath entries necessary both for our compiler calls and for downstream JVM tasks.
    for conf in self._confs:
      cp.insert(0, (conf, self._resources_dir))
      for jar in self._zinc_utils.plugin_jars():
        cp.insert(0, (conf, jar))

    # Invalidation check. Everything inside the with block must succeed for the
    # invalid targets to become valid.
    with self.invalidated(scala_targets, invalidate_dependents=True,
                          partition_size_hint=self._partition_size_hint) as invalidation_check:
      all_sources_by_target = {}
      # Process partitions of invalid targets one by one.
      for vts in invalidation_check.invalid_vts_partitioned:
        if not self.dry_run:
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
      egroups.update_compatible_classpaths(exclusives_key, [(conf, self._classes_dir)])

  # def _localize_portable_analysis_files(self, vts):
  #   # Localize the analysis files we read from the artifact cache.
  #   for vt in vts:
  #     analysis_file = self._artifact_factory.analysis_file_for_targets(vt.targets)
  #     if self._zinc_utils.localize_analysis_file(
  #         ZincArtifactFactory.portable(analysis_file.analysis_file), analysis_file.analysis_file):
  #       self.context.log.warn('Zinc failed to localize analysis file: %s. Incremental rebuild' \
  #                             'of that target may not be possible.' % analysis_file)

  def _write_to_artifact_cache(self, vts, sources_by_target):
    analysis_dir = os.path.join(self._analysis_dir,
                                Target.maybe_readable_identify(sources_by_target.keys()))
    try:
      vt_by_target = dict([(vt.target, vt) for vt in vts.versioned_targets])

      # Copy the analysis file, so we can work on it without it changing under us.
      analysis_file_copy = os.path.join(analysis_dir, 'analysis')
      shutil.copyfile(self._analysis_file, analysis_file_copy)

      # This work can happen in the background, assiming analysis_dir isn't cleaned up.
      analysis_file_portable = analysis_file_copy + '.portable'
      if self._zinc_utils.relativize_analysis_file(analysis_file_copy, analysis_file_portable):
        raise TaskError('Zinc failed to relativize analysis file: %s' % analysis_file_copy)
      classes_by_source = self._compute_classes_by_source(analysis_file_copy)

      def analysis_for_target(target):
        return target.id + '.analysis.portable'

      # Split the analysis into per-target files.
      splits = [(sources, analysis_for_target(target))
                for target, sources in sources_by_target.items()]
      self._zinc_utils.run_zinc_split(analysis_file_portable, splits)

      # Gather up the artifacts.
      vts_artifactfiles_pairs = []
      for target, sources in sources_by_target.items():
        artifacts = [analysis_for_target(target)]
        for source in sources:
          artifacts.extend(classes_by_source.get(source, []))
        vt = vt_by_target.get(target)
        if vt is not None:
          vts_artifactfiles_pairs.append((vt, artifacts))

      # Write to the artifact cache.
      self.update_artifact_cache(vts_artifactfiles_pairs)
    except:
      shutil.rmtree(analysis_dir)
      raise

  def check_artifact_cache(self, vts):
    # Special handling for scala artifacts.
    return [], []
    # cached_vts, uncached_vts = Task.check_artifact_cache(self, vts)
    #
    # if cached_vts:
    #   # Localize the portable analysis files.
    #   with self.context.new_workunit('localize', labels=[WorkUnit.MULTITOOL]):
    #     self._localize_portable_analysis_files(cached_vts)
    #
    #   # Split any merged artifacts.
    #   for vt in cached_vts:
    #     if len(vt.targets) > 1:
    #       artifacts = [self._artifact_factory.artifact_for_target(t) for t in vt.targets]
    #       merged_artifact = self._artifact_factory.merged_artifact(artifacts)
    #       merged_artifact.split()
    #       for v in vt.versioned_targets:
    #         v.update()
    # return cached_vts, uncached_vts

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

      write_to_artifact_cache = self._artifact_cache and \
                                self.context.options.write_to_artifact_cache

      #if write_to_artifact_cache:
      #  # Write the entire merged artifact, and each individual split artifact,
      #  # to the artifact cache, if needed.
      #  vts_artifact_pairs = zip(vts.versioned_targets, artifacts) + [(vts, merged_artifact)]
      #  self._update_artifact_cache(vts_artifact_pairs)

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

  # def _update_artifact_cache(self, vts_artifact_pairs):
  #   # Relativize the analysis.
  #   vts_artifactfiles_pairs = []
  #   with self.context.new_workunit(name='cacheprep'):
  #     with self.context.new_workunit(name='relativize', labels=[WorkUnit.MULTITOOL]):
  #       for vts, artifact in vts_artifact_pairs:
  #         if os.path.exists(artifact.analysis_file) and \
  #             self._zinc_utils.relativize_analysis_file(artifact.analysis_file,
  #                                                       artifact.portable_analysis_file):
  #           raise TaskError('Zinc failed to relativize analysis file: %s' % artifact.analysis_file)
  #         artifact_files = [artifact.classes_dir, artifact.portable_analysis_file]
  #         vts_artifactfiles_pairs.append((vts, artifact_files))
  #
  #   self.update_artifact_cache(vts_artifactfiles_pairs)
