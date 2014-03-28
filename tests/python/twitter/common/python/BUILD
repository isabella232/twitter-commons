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


python_test_suite(name = 'all',
  dependencies = [
    'tests/python/twitter/common/python/http:all',
    ':test_environment',
    ':test_finders',
    ':test_interpreter',
    ':test_obtainer',
    ':test_package',
    ':test_pep425',
    ':test_platform',
    ':test_pex_builder',
    ':test_util',
  ]
)

python_tests(name = 'test_environment',
  sources = ['test_environment.py'],
  dependencies = [
    'src/python/twitter/common/contextutil',
    'src/python/twitter/common/python',
  ]
)

python_tests(name = 'test_finders',
  sources = ['test_finders.py'],
  dependencies = [
    'src/python/twitter/common/python',
  ],
  requirements = [
    python_requirement('mock', version_filter=lambda py, plat: py.startswith('2')),
  ],
)

python_tests(name = 'test_interpreter',
  sources = ['test_interpreter.py'],
  dependencies = [
    '3rdparty/python:mock',
    'src/python/twitter/common/python',
  ]
)

python_tests(name = 'test_obtainer',
  sources = ['test_obtainer.py'],
  dependencies = [
    'src/python/twitter/common/python',
  ]
)

python_tests(name = 'test_package',
  sources = ['test_package.py'],
  dependencies = [
    'src/python/twitter/common/contextutil',
    'src/python/twitter/common/python',
  ]
)

python_tests(name = 'test_pep425',
  sources = ['test_pep425.py'],
  dependencies = [
    'src/python/twitter/common/python',
  ]
)

python_tests(name = 'test_platform',
  sources = ['test_platform.py'],
  dependencies = [
    'src/python/twitter/common/python'
  ]
)

python_tests(name = 'test_pex_builder',
  sources = ['test_pex_builder.py'],
  dependencies = [
    'src/python/twitter/common/contextutil',
    'src/python/twitter/common/python',
  ]
)

python_tests(name = 'test_util',
  sources = ['test_util.py'],
  dependencies = [
    'src/python/twitter/common/contextutil',
    'src/python/twitter/common/python',
  ]
)
