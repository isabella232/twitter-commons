

class MustacheRenderer(object):
  """Renders text using mustache templates."""

  @staticmethod
  def expand(args):
    # Add foo? for each foo in the map that evaluates to true.
    # Mustache needs this, especially in cases where foo is a list: there is no way to render a
    # block exactly once iff a list is not empty.
    # Note: if the original map contains foo?, it will take precedence over our synthetic foo?.
    def convert_val(x):
      # Pystache can't handle sets, so we convert to maps of key->True.
      if isinstance(x, set):
        return dict([(k, True) for k in x])
      elif isinstance(x, dict):
        return MustacheRenderer.expand(x)
      elif isinstance(x, list):
        return [convert_val(e) for e in x]
      else:
        return x
    items = [(key, convert_val(val)) for (key, val) in args.items()]
    ret = dict([(key + '?', True) for (key, val) in items if val and not key.endswith('?')])
    ret.update(dict(items))
    return ret

  def __init__(self, pystache_renderer):
    self._pystache_renderer = pystache_renderer

  def render_name(self, template_name, args):
    return self._pystache_renderer.render_name(template_name, MustacheRenderer.expand(args))

  def render(self, template, args):
    return self._pystache_renderer.render(template, MustacheRenderer.expand(args))

