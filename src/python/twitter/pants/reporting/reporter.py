

class Reporter(object):
  def __init__(self, run_tracker):
    self.run_tracker = run_tracker
    self.formatter = None

  def open(self):
    pass

  def close(self):
    pass

  def start_workunit(self, workunit):
    pass

  def handle_output(self, workunit, label, s):
    """label - classifies the output e.g., 'stdout' for output captured from a tool's stdout.
    Other labels are possible, e.g., if we capture output from a tool's logfiles.
    """
    pass

  def handle_message(self, workunit, *msg_elements):
    pass

