
from multiprocessing.pool import ThreadPool

class WorkerPool(object):
  """A pool of workers.

  Workers are threads, and so are subject to GIL constraints. Submitting CPU-bound work
  may not be effective. Use this class primarily for IO-bound work.
  """

  def __init__(self, name, run_tracker, num_workers):
    self._run_tracker = run_tracker
    # All workers accrue work to the same root.
    self._pool = ThreadPool(processes=num_workers,
                            initializer=self._run_tracker.register_root, initargs=(name, ))
    self._shutdown_hooks = []

  def add_shutdown_hook(self, hook):
    self._shutdown_hooks.append(hook)

  def submit_async_work(self, func, args_tuples, workunit_name=None, callback=None):
    """Submit work to be executed in the background.

    - func: a callable.
    - args_tuples: an iterable of argument tuples for func. func will be called once per arg tuple.
    - workunit_name: If specified, each invocation will be executed in a workunit of this name.
    - callback: If specified, a callable taking a single argument, which will be a list
                of return values of each invocation, in order. Called only if all work succeeded.
    """
    if len(args_tuples) == 0:  # map_async hangs on 0-length iterables.
      if callback:
        callback([])
    else:
      def do_work(*args):
        self._do_work(func, *args, workunit_name=workunit_name)
      self._pool.map_async(do_work, args_tuples, chunksize=1, callback=callback)

  def submit_work_and_wait(self, func, args_tuples, workunit_name=None):
    """Submit work to be executed on this pool, but wait for it to complete.

    - func: a callable.
    - args_tuples: an iterable of argument tuples for func. func will be called once per arg tuple.
    - workunit_name: If specified, each invocation will be executed in a workunit of this name.

    Returns a list of return values of each invocation, in order.  Throws if any invocation does.
    """
    if len(args_tuples) == 0:  # map hangs on 0-length iterables.
      return []
    else:
      def do_work(*args):
        self._do_work(func, *args, workunit_name=workunit_name)
      return self._pool.map(do_work, args_tuples, chunksize=1)

  def _do_work(self, func, args_tuple, workunit_name):
    if workunit_name:
      with self._run_tracker.new_workunit(name=workunit_name):
        return func(*args_tuple)
    else:
      return func(*args_tuple)

  def shutdown(self):
    self._pool.close()
    self._pool.join()
    for hook in self._shutdown_hooks:
      hook()