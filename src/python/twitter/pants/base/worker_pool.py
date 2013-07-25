
from multiprocessing.pool import ThreadPool

class WorkerPool(object):
  """A pool of workers.

  Workers are threads, and so are subject to GIL constraints. Submitting CPU-bound work
  may not be effective. Use this class primarily for IO-bound work.
  """

  def __init__(self, context, num_workers):
    self._context = context
    self._pool = ThreadPool(processes=num_workers)

  def submit_async_work(self, func, args_tuples, workunit_name=None, callback=None):
    """Submit work to be executed in the background.

    - func: a callable.
    - args_tuples: an iterable of argument tuples for func. func will be called once per arg tuple.
    - workunit_name: If specified, all the work will be executed in a single workunit of this name.
    - callback: If specified, a callable taking a single argument, which will be a list
                of return values of each invocation, in order. Called only if all work succeeded.
    """
    # TODO: Support workunit tracking in an async context.
    self._pool.map_async(func, args_tuples, chunksize=1, callback=callback)

  def submit_work_and_wait(self, func, args_tuples, workunit_name=None):
    """Submit work to be executed on this pool, but wait for it to complete.

    - func: a callable.
    - args_tuples: an iterable of argument tuples for func. func will be called once per arg tuple.
    - workunit_name: If specified, all the work will be executed in a single workunit of this name.

    Returns a list of return values of each invocation, in order.  Throws if any invocation does.
    """
    def do_work():
      return self._pool.map(func, args_tuples, chunksize=1)

    if workunit_name:
      with self._context.new_workunit(name=workunit_name):
        return do_work()
    else:
      return do_work()

  def stop(self):
    self._pool.close()
    self._pool.join()