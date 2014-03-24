java_library(name = 'http',
  provides = artifact(
    org = 'com.twitter.common',
    name = 'net-http',
    repo = 'build-support/commons/ivy:public',
  ),
  dependencies = [
    '3rdparty:guava',
    '3rdparty:guice',
    '3rdparty:guice-servlet',
    '3rdparty:jetty',
    '3rdparty:jsr305',
    '3rdparty:servlet-api',
    'src/java/com/twitter/common/base',
    'src/java/com/twitter/common/net/http/handlers:text',
    'src/java/com/twitter/common/util:system-mocks',
  ],
  sources = globs('*.java')
)
