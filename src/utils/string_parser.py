import typing as t
import inflect

e = inflect.engine()

def _pluralize(name: str, label: t.Optional[str]) -> str:
    if name is not None and label is not None:
        if len(label) > 0 and label not in ['pants', 'shorts']:
            name = e.plural(name)
    return name

def _process_smart_tags(tags: t.List[dict]) -> t.List[dict]:
    return [ {**t, "suggestion": _pluralize(t['suggestion'], t['product_label']) }
            for t in tags ]

def process_boards(boards: t.List[dict]) -> t.List[dict]:
    parsed_boards = [ {**b, "smart_tags": _process_smart_tags(b['smart_tags']) }
            for b in boards ]
    return parsed_boards

def process_suggested_boards(boards: t.List[dict]) -> t.List[dict]:
    parsed_boards = [ {**b, "name": _pluralize(b['name'], b['product_label'])} for b in boards ]
    return parsed_boards 

def convert_product_ids_to_string(products: t.List[dict]) -> t.List[dict]:
    for product in products:
        product['product_id'] = str(product['product_id'])
    return products 
