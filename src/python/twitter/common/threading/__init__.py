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

import threading
import time


class StoppableThread(threading.Thread):
  """A thread that can be stopped.

  The target function will be called in a tight loop until the thread is stopped.

  Note: Don't subclass this to override run(). That won't work. """
  def __init__(self, group=None, target=None, name=None, post_target=None, args=(), kwargs=None):
    if kwargs is None:
      kwargs = {}

    def stoppable_target():
      while True:
        target(*args, **kwargs)
        if post_target:
          post_target()
        with self._lock:
          if self._stopped:
            return

    threading.Thread.__init__(self, group=group, target=stoppable_target, name=name, args=args, kwargs=kwargs)
    self._lock = threading.Lock()  # Protects self._stopped.
    self._stopped = False

  def stop(self):
    with self._lock:
      self._stopped = True
    self.join()


class PeriodicThread(StoppableThread):
  """A thread that runs a target function periodically.

  Note: Don't subclass this to override run(). That won't work. """
  def __init__(self, group=None, target=None, name=None, period_secs=1, args=(), kwargs=None):
    if kwargs is None:
      kwargs = {}

    def periodic_target():
      target(*args, **kwargs)
      time.sleep(period_secs)

    StoppableThread.__init__(self, group=group, target=periodic_target, name=name, args=args, kwargs=kwargs)
