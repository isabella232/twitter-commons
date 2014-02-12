import os
from twitter.common.collections import OrderedSet


class ClassPathElement(object):
  pass


class SimpleClassPathElement(ClassPathElement):
  def __init__(self, path):
    self._path = path

  def __eq__(self, other):
    if type(self) == type(other):
      return self._path == other._path
    else:
      return False

  def __ne__(self, other):
    return not self == other

  def __hash__(self):
    return hash(self._path)

  def __repr__(self):
    return '%s(%s)' % (type(self), self._path)

  def __str__(self):
    return self._path

class JarFile(SimpleClassPathElement):
  pass

class Directory(SimpleClassPathElement):
  pass


class FilteredClassPathElement(ClassPathElement):
  def __init__(self, underlying_element, whitelist_path=None, blacklist_path=None):
    self._underlying_element = underlying_element
    self._whitelist_path = whitelist_path
    self._blacklist_path = blacklist_path

  def __eq__(self, other):
    if type(self) == type(other):
      return self._underlying_element == other._underlying_element \
         and self._whitelist_path == other._whitelist_path \
         and self._blacklist_path == other._blacklist_path
    else:
      return False

  def __ne__(self, other):
    return not self == other

  def __hash__(self):
    return hash(self._underlying_element) ^ hash(self._whitelist_path) ^ hash(self._blacklist_path)

  def __repr__(self):
    return '%s(%s, whitelist_path=%s, blacklist_path=%s)' % \
           (type(self), self._underlying_element, self._whitelist_path, self._blacklist_path)

  def __str__(self):
    return self._underlying_element


class ClassPath(object):
  def __init__(self, initial_elements=None):
    self._elements = OrderedSet(initial_elements or [])

  def add_element(self, element):
    if not isinstance(element, ClassPathElement):
      raise Exception('Argument must be a ClassPathElement.')
    self._elements.add(element)

  def get_unfiltered_classpath_element_strings(self):
    return [str(x) for x in self._elements if not isinstance(x, FilteredClassPathElement)]

  def get_all_classpath_element_strings(self):
    return map(str, self._elements)

  def get_filtered_classpath_args(self):
    def join(elements):
      return os.path.pathsep.join(elements)

    filtered_classpath = []
    whitelists = OrderedSet()
    blacklists = OrderedSet()

    for element in self._elements:
      if isinstance(element, FilteredClassPathElement):
        filtered_classpath.append(str(element))
        if element._whitelist_path:
          whitelists.add(element._whitelist_path)
        if element._blacklist_path:
          blacklists.add(element._blacklist_path)

    args = []
    if filtered_classpath:
      args.append('-Dpants.class.path.filtered=%s' % join(filtered_classpath))
      if whitelists:
        args.append('-Dpants.classloader.whitelists=%s' % join(whitelists))
      if blacklists:
        args.append('-Dpants.classloader.blacklists=%s' % join(blacklists))
    return args

  def __eq__(self, other):
    if type(self) == type(other):
      return self._elements == other._elements
    else:
      return False

  def __ne__(self, other):
    return not self == other

  def __hash__(self):
    return hash(self._elements)

  def __repr__(self):
    return '%s(%s)' % (type(self), os.path.pathsep.join([repr(x) for x in self._elements]))

  def __str__(self):
    return os.path.pathsep.join(self._elements)
