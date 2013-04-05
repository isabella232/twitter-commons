
def list_to_report_element(items, item_type):
  items = [str(x) for x in items]
  return '%d %s%s' % (len(items), item_type, 's' if len(items) > 1 else ''), '\n'.join(items)
