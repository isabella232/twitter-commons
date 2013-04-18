

def list_to_report_element(items, item_type):
  def pluralize(x):
    if x.endswith('s'):
      return x + 'es'
    else:
      return x + 's'

  items = [str(x) for x in items]
  n = len(items)
  text = '%d %s' % (n, item_type if n == 1 else pluralize(item_type))
  if n == 0:
    return text
  else:
    detail = '\n'.join(items)
    return text, detail
