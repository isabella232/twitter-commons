java_library(name = 'pprof',
  provides = artifact(
    org = 'com.twitter.common',
    name = 'net-http-handlers-pprof',
    repo = 'build-support/commons/ivy:public',
  ),
  dependencies = [
    '3rdparty:guava',
    '3rdparty:servlet-api',
    '3rdparty:util-core',
    '3rdparty:util-jvm',
    'src/java/com/twitter/common/net/http/handlers:params',
    'src/java/com/twitter/common/quantity',
  ],
  sources = globs('*.java')
)
