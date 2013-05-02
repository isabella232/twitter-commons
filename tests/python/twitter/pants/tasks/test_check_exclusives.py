
__author__ = 'Mark Chu-Carroll (markcc@foursquare.com)'

import unittest

from twitter.pants.base import Config
from twitter.pants.goal import Context
from twitter.pants.testutils import MockTarget
from twitter.pants.tasks import TaskError
from twitter.pants.tasks.check_exclusives import CheckExclusives

class CheckExclusivesTest(unittest.TestCase):
  """Test of the CheckExclusives task."""

  @classmethod
  def setUpClass(cls):
     cls.config = Config.load()

  def setupTargets(self):
    a = MockTarget('a', exclusives={'a': '1', 'b': '1'})
    b = MockTarget('b', exclusives={'a': '1'})
    c = MockTarget('c', exclusives = {'a': '2'})
    d = MockTarget('d', dependencies=[a, b])
    e = MockTarget('e', dependencies=[a, c], exclusives={'c': '1'})
    return a, b, c, d, e

  def test_check_exclusives(self):
    a, b, c, d, e = self.setupTargets()
    context = Context(CheckExclusivesTest.config, options={}, target_roots=[d, e])
    check_exclusives_task = CheckExclusives(context, signal_error=True)


    try:
      check_exclusives_task.execute([d, e])
      self.fail("Expected a conflicting exclusives exception to be thrown.")
    except TaskError:
      pass

