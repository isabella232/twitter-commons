import os
import pystache



class RendererError(Exception):
  def __init__(self, msg):
    Exception.__init__(self, msg)

class Renderer(object):
  ext = '.mustache'

  def __init__(self, template_dir, require=list()):
    """A helper that renders mustache templates.

    template_dir - a directory of templates that can subsequently be referenced by name.
                  E.g., foo.mustache can be referenced as foo.

    require - a list of template names that must be present in template_dir for initialization of
              this Renderer to succeed.
    """
    self.templates = {}  # Map from template name (e.g., foo for foo.mustache) to full path of template file.

    # Populate templates with whatever we find in template_dir.
    for name_with_ext in filter(lambda x: x.endswith(Renderer.ext), os.listdir(template_dir)):
      name = name_with_ext[0:-len(Renderer.ext)]
      path = os.path.join(template_dir, name_with_ext)
      self.templates[name] = path

    # Check that we have the templates we need.
    for name in require:
      if not self.has_template(name):
        raise RendererError, 'Template missing. Expected %s/%s%s' % (template_dir, name, Renderer.ext)

  def render(self, template_name, args):
    with open(self.templates.get(template_name), 'r') as infile:
      template = infile.read()
    return pystache.render(template, args)

  def has_template(self, template_name):
    return template_name in self.templates
