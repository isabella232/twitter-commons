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
    ':test_crawler',
    ':test_http',
  ]
)

python_tests(
  name = 'test_crawler',
  sources = ['test_crawler.py'],
  dependencies = [
    'src/python/twitter/common/contextutil',
    'src/python/twitter/common/python',
  ]
)

def mock_filter(python, platform):
  return python.startswith('2')

python_tests(
  name = 'test_http',
  sources = ['test_http.py'],
  dependencies = [
    'src/python/twitter/common/contextutil',
    'src/python/twitter/common/python',
    'src/python/twitter/common/quantity',
    'src/python/twitter/common/testing',
  ],
  requirements = [
    python_requirement('mock', version_filter=mock_filter),
  ],
)
