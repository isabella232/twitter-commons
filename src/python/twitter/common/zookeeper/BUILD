TEST_SERVER = ['test_server.py']

python_library(
  name = 'zookeeper',
  sources = globs('*.py') - TEST_SERVER,
  dependencies = [
    '3rdparty/python:zc-zookeeper-static',
    'src/python/twitter/common',
  ],
)

python_library(
  name = 'kazoo_client',
  sources = ['kazoo_client.py'],
  dependencies = [
    '3rdparty/python:kazoo',
    'src/python/twitter/common/metrics',
  ]
)

python_library(
  name = 'testing',
  sources = TEST_SERVER,
  dependencies = [
    'src/python/twitter/common/contextutil',
    'src/python/twitter/common/dirutil',
    'src/python/twitter/common/rpc/finagle',
    'src/thrift/com/twitter/common/zookeeper/testing/angrybird:py-thrift',
  ]
)

python_library(
  name = 'zookeeper-packaged',
  dependencies = [
    ':kazoo_client',
    'src/python/twitter/common/zookeeper/group:kazoo_group',
    'src/python/twitter/common/zookeeper/serverset:kazoo_serverset',
  ],
  provides = setup_py(
    name = 'twitter.common.zookeeper',
    version = '0.1.1',
    description = "Implementations of Twitter's service discovery libraries on top of Kazoo."
  )
)
