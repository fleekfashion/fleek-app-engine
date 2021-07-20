import typing as t
import inflect

e = inflect.engine()

def _pluralize_board_name(board: dict) -> dict:
    name = board['name']
    label = board['product_label']
    if name is not None and len(label) > 0 and label not in ['pants', 'shorts']:
        name = e.plural(name)
    return {**board, "name": name}

def process_boards(boards: t.List[dict]) -> t.List[dict]:
    parsed_boards = [ _pluralize_board_name(b) for b in boards ]
    return parsed_boards 
