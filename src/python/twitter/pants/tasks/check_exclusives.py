__author__ = 'Mark Chu-Carroll (markcc@foursquare.com)'

from collections import namedtuple
from twitter.pants.tasks import Task, TaskError

class CheckExclusives(Task):
  """Task for computing transitive exclusive maps.

  This computes transitive exclusive tags for a dependency graph rooted
  with a set of build targets specified by a user. If this process produces
  any collisions where a single target contains multiple tag values for a single
  exclusives key, then it generates an error and the compilation will fail.

  The syntax of the exclusives attribute is:
      exclusives = {"id": "value", ...}

  For example, suppose that we had two java targets, jliba and jlibb,
  which used different versions of joda datetime.

    java_library(name='jliba',
       depedencies = ['joda-1.5'])
    java_library(name='jlibb',
       dependencies=['joda-2.1'])
    java_binary(name='javabin', dependencies=[':jliba', ':jlibb'])

  In this case, the binary target 'javabin' depends on both joda-1.5 and joda-2.1.
  This should be a build error, but pants doesn't know that joda-1.5 and joda-2.1 are
  different versions of the same library, and so it can't detect the error.

  With exclusives, the jar_library target for the joda libraries would declare
  exclusives tags:

    jar_library(name='joda-1.5', exclusives={'joda': '1.5'})
    jar_library(name='joda-2.1', exclusives={'joda': '2.1'})

  With the exclusives declared, pants can recognize that 'javabin' has conflicting
  dependencies, and can generate an appropriate error message.
  """

  @classmethod
  def setup_parser(cls, option_group, args, mkflag):
    Task.setup_parser(option_group, args, mkflag)
    option_group.add_option(mkflag('error_on_collision'),
                            mkflag('error_on_collision', negate=True),
                            dest='exclusives_error_on_collision', default=True,
                            action='callback', callback=mkflag.set_bool,
                            help=("[%default] Signal an error and abort the build if an " +
                                  "exclusives collision is detected"))

  def __init__(self, context, signal_error=None):
    Task.__init__(self, context)
    self.signal_error = (context.options.exclusives_error_on_collision
                         if signal_error is None else signal_error)

  def execute(self, targets):
    # compute transitive exclusives
    for t in targets:
      t._propagate_exclusives()
    # Check for exclusives collision.
    for t in targets:
      excl = t.get_all_exclusives()
      for key in excl:
        if len(excl[key]) > 1:
          msg = 'target %s has more than 1 exclusives tag for key %s' % (t, key)
          if self.signal_error:
            raise TaskError(msg)
          else:
            print "Warning: %s" % msg



