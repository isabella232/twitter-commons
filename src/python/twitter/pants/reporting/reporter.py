
class Reporter(object):
  """Formats and emits reports.

  Subclasses implement the callback methods, to provide specific reporting
  functionality, e.g., to console or to browser.
  """
  def __init__(self, run_tracker, settings):
    self.run_tracker = run_tracker
    self.update_settings(settings)

  def update_settings(self, settings):
    """Modify reporting settings once we've got cmd-line flags etc.

    A subclass will ignore settings objects not of the expected type, so its safe
    to pass any settings here."""
    if isinstance(settings, type(self).Settings):
      self.settings = settings

  def open(self):
    """Begin the report."""
    pass

  def close(self):
    """End the report."""
    pass

  def start_workunit(self, workunit):
    """A new workunit has started."""
    pass

  def end_workunit(self, workunit):
    """A workunit has finished."""
    pass

  def handle_log(self, workunit, level, *msg_elements):
    """Handle a message logged by pants code.

    level: One of the constants above.

    Each element in msg_elements is either a message or a (message, detail) pair.
    A subclass must show the message, but may choose to show the detail in some
    sensible way (e.g., when the message text is clicked on in a browser).
    """
    pass

  def handle_output(self, workunit, label, s):
    """Handle output captured from an invoked tool (e.g., javac).

    workunit: The innermost WorkUnit in which the tool was invoked.
    label: Classifies the output e.g., 'stdout' for output captured from a tool's stdout or
           'debug' for debug output captured from a tool's logfiles.
    s: The content captured.
    """
    pass
