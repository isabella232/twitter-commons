# ==================================================================================================
# Copyright 2012 Twitter, Inc.
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

import getpass
import socket
import subprocess

from collections import namedtuple
from time import localtime, strftime, time

from twitter.pants import get_buildroot, get_scm


def safe_call(cmd):
  po = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  so, se = po.communicate()
  if po.returncode == 0:
    return so
  return ""


BuildInfo = namedtuple('BuildInfo', 'epochtime date time timestamp branch tag sha user machine path')


def get_build_info(scm=None, epochtime=None):
  """Calculates the current BuildInfo using the supplied scm or else the globally configured one."""
  buildroot = get_buildroot()
  scm = scm or get_scm()

  epochtime = epochtime or time()
  now = localtime(epochtime)
  revision = scm.commit_id
  tag = scm.tag_name or 'none'
  branchname = scm.branch_name or revision

  return BuildInfo(
    epochtime=epochtime,  # A double, so we get subsecond precision for id purposes.
    date=strftime('%A %b %d, %Y', now),
    time=strftime('%H:%M:%S', now),
    timestamp=strftime('%m.%d.%Y %H:%M', now),
    branch=branchname,
    tag=tag,
    sha=revision,
    user=getpass.getuser(),
    machine=socket.gethostname(),
    path=buildroot)
