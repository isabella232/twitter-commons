
__author__ = 'Mark C. Chu-Carroll (markcc@foursquare.com()'

from twitter.pants.testutils import MockTarget
from twitter.pants.goal.group import Group
import unittest

class ExclusivesTargetTest(unittest.TestCase):
  """Test exclusives propagation in the dependency graph"""
  def setupTargets(self):
    a = MockTarget('a', exclusives={'a': '1', 'b': '1'})
    b = MockTarget('b', exclusives={'a': '1'})
    c = MockTarget('c', exclusives = {'a': '2'})
    d = MockTarget('d', dependencies=[a, b])
    e = MockTarget('e', dependencies=[a, c], exclusives={'c': '1'})
    return a, b, c, d, e

  def testPropagation(self):
    a, b, c, d, e = self.setupTargets()
    d_excl = d.get_all_exclusives()
    self.assertEquals(d_excl['a'], set(['1']))
    e_excl = e.get_all_exclusives()
    self.assertEquals(e_excl['a'], set(['1', '2']))

  def testPartitioning(self):
    # Target e has conflicts; in this test, we want to check that partitioning
    # of valid targets works to prevent conflicts in chunks.
    a, b, c, d, _ = self.setupTargets()
    a._propagate_exclusives()
    b._propagate_exclusives()
    c._propagate_exclusives()
    d._propagate_exclusives()

    parts = Group.compute_exclusives_chunks([a, b, c, d])
    self.assertEquals(parts["['1']"], [a, b, d])
    self.assertEquals(parts["['2']"], [c])
    self.assertEquals(2, len(parts))



