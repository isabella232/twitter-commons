from twitter.pants.base import Config
from twitter.pants.goal import Context
from twitter.pants.tasks.jvm_compile.classpath import Directory, ClassPath
from twitter.pants.testutils import MockTarget
from twitter.pants.tasks import TaskError
from twitter.pants.tasks.check_exclusives import CheckExclusives
from twitter.pants.testutils.base_mock_target_test import BaseMockTargetTest


class CheckExclusivesTest(BaseMockTargetTest):
  """Test of the CheckExclusives task."""
  config = None

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
    context = Context(CheckExclusivesTest.config, options={}, run_tracker=None, target_roots=[d, e])
    check_exclusives_task = CheckExclusives(context, signal_error=True)
    try:
      check_exclusives_task.execute([d, e])
      self.fail("Expected a conflicting exclusives exception to be thrown.")
    except TaskError:
      pass

  def test_classpath_compatibility(self):
    # test the compatibility checks for different exclusive groups.
    a = MockTarget('a', exclusives={'a': '1', 'b': '1'})
    b = MockTarget('b', exclusives={'a': '1', 'b': '<none>'})
    c = MockTarget('c', exclusives = {'a': '2', 'b': '2'})
    d = MockTarget('d')

    context = Context(CheckExclusivesTest.config, options={}, run_tracker=None, target_roots=[a, b, c, d])
    context.products.require_data('exclusives_groups')
    check_exclusives_task = CheckExclusives(context, signal_error=True)
    check_exclusives_task.execute([a, b, c, d])
    egroups = context.products.get_data('exclusives_groups')
    # Expected compatibility:
    # a is compatible with nothing but itself.
    self.assertTrue(egroups._is_compatible(egroups.target_to_key[a], egroups.target_to_key[a]))
    self.assertFalse(egroups._is_compatible(egroups.target_to_key[a], egroups.target_to_key[b]))
    self.assertFalse(egroups._is_compatible(egroups.target_to_key[a], egroups.target_to_key[d]))
    self.assertFalse(egroups._is_compatible(egroups.target_to_key[a], egroups.target_to_key[c]))

    # b is compatible with itself and a.
    self.assertTrue(egroups._is_compatible(egroups.target_to_key[b], egroups.target_to_key[a]))
    self.assertTrue(egroups._is_compatible(egroups.target_to_key[b], egroups.target_to_key[b]))
    self.assertFalse(egroups._is_compatible(egroups.target_to_key[b], egroups.target_to_key[c]))
    self.assertFalse(egroups._is_compatible(egroups.target_to_key[b], egroups.target_to_key[d]))

    # c is compatible with nothing but itself
    self.assertTrue(egroups._is_compatible(egroups.target_to_key[c], egroups.target_to_key[c]))
    self.assertFalse(egroups._is_compatible(egroups.target_to_key[c], egroups.target_to_key[a]))
    self.assertFalse(egroups._is_compatible(egroups.target_to_key[c], egroups.target_to_key[b]))
    self.assertFalse(egroups._is_compatible(egroups.target_to_key[c], egroups.target_to_key[d]))

    # d is compatible with everything.
    self.assertTrue(egroups._is_compatible(egroups.target_to_key[d], egroups.target_to_key[a]))
    self.assertTrue(egroups._is_compatible(egroups.target_to_key[d], egroups.target_to_key[b]))
    self.assertTrue(egroups._is_compatible(egroups.target_to_key[d], egroups.target_to_key[c]))
    self.assertTrue(egroups._is_compatible(egroups.target_to_key[d], egroups.target_to_key[d]))


  def testClasspathUpdates(self):
    # Check that exclusive groups classpaths accumulate properly.
    a = MockTarget('a', exclusives={'a': '1', 'b': '1'})
    b = MockTarget('b', exclusives={'a': '1', 'b': '<none>'})
    c = MockTarget('c', exclusives = {'a': '2', 'b': '2'})
    d = MockTarget('d')

    context = Context(CheckExclusivesTest.config, options={}, run_tracker=None, target_roots=[a, b, c, d])
    context.products.require_data('exclusives_groups')
    check_exclusives_task = CheckExclusives(context, signal_error=True)
    check_exclusives_task.execute([a, b, c, d])
    egroups = context.products.get_data('exclusives_groups')

    def make_cp(raw_elements):
      return ClassPath([Directory(x) for x in raw_elements])

    egroups._set_base_classpath_for_group("a=1,b=1", make_cp(["a1", "b1"]))
    egroups._set_base_classpath_for_group("a=1,b=<none>", make_cp([ "a1" ]))
    egroups._set_base_classpath_for_group("a=2,b=2", make_cp([ "a2", "b2"]))
    egroups._set_base_classpath_for_group("a=<none>,b=<none>", make_cp(["none"]))
    egroups.add_to_compatible_classpaths(None, Directory("update_without_group"))
    egroups.add_to_compatible_classpaths("a=<none>,b=<none>", Directory("update_all"))
    egroups.add_to_compatible_classpaths("a=1,b=<none>", Directory("update_a1bn"))
    egroups.add_to_compatible_classpaths("a=2,b=2", Directory("update_only_a2b2"))
    self.assertEquals(egroups.get_classpath_for_group("a=2,b=2"),
        make_cp([ "a2", "b2", "update_without_group", "update_all", "update_only_a2b2"]))
    self.assertEquals(egroups.get_classpath_for_group("a=1,b=1"),
        make_cp([ "a1", "b1", "update_without_group", "update_all", "update_a1bn" ]))
    self.assertEquals(egroups.get_classpath_for_group("a=1,b=<none>"),
        make_cp([ "a1", "update_without_group", "update_all", "update_a1bn" ]))
    self.assertEquals(egroups.get_classpath_for_group("a=<none>,b=<none>"),
        make_cp([ "none", "update_without_group", "update_all" ]))

    # make sure repeated additions of the same thing are idempotent.
    egroups.add_to_compatible_classpaths("a=1,b=1", Directory("a1"))
    egroups.add_to_compatible_classpaths("a=1,b=1", Directory("b1"))
    egroups.add_to_compatible_classpaths("a=1,b=1", Directory("xxx"))
    self.assertEquals(egroups.get_classpath_for_group("a=1,b=1"),
        make_cp([ "a1", "b1", "update_without_group", "update_all", "update_a1bn", "xxx" ]))






