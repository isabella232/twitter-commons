python_library(
  name = 'serverset_base',
  sources = globs('*.py') - ['cli.py'],
  dependencies = [
    'src/python/twitter/common/lang',
    'src/python/twitter/common/zookeeper/group:group_base',
    'src/thrift/com/twitter/thrift:py-thrift',
  ]
)

python_library(
  name = 'serverset',
  dependencies = [
    ':serverset_base',
    'src/python/twitter/common/zookeeper/group',
    'src/python/twitter/common/zookeeper',
  ]
)

python_library(
  name = 'kazoo_serverset',
  dependencies = [
    ':serverset_base',
    'src/python/twitter/common/zookeeper/group:kazoo_group',
  ]
)

python_binary(
  name = 'cli',
  source = 'cli.py',
  dependencies = [
    'src/python/twitter/common/app',
    ':serverset',
  ],
)
