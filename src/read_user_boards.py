from src.utils.sqlalchemy_utils import load_session, row_to_dict
from src.utils import hashers
from src.defs import postgres as p
from datetime import datetime as dt


def getBoardInfo(args: dict) -> dict:
    board_id = args['board_id']

    session = load_session()
    board = session.query(p.Board).filter(p.Board.board_id == board_id).first()

    return row_to_dict(board) if board else {'success': False}