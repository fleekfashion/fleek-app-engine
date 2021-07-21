from src.defs.types.board_type import BoardType
from src.utils.board_utils import get_insert_board_on_board_type_not_exists_statements
from src.utils.sqlalchemy_utils import session_scope

def db_initialize(args: dict) -> dict:
    price_drop_board_insert_stmts = get_insert_board_on_board_type_not_exists_statements(args, BoardType.PRICE_DROP)

    try:
        with session_scope() as session:
            for stmt in price_drop_board_insert_stmts: session.execute(stmt)
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True}
