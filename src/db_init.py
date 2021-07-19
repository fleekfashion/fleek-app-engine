from src.price_drop import get_price_drop_board_insert_stmts
from src.utils.sqlalchemy_utils import session_scope

def db_initialize(args: dict) -> dict:
    price_drop_board_insert_stmts = get_price_drop_board_insert_stmts(args)

    try:
        with session_scope() as session:
            for stmt in price_drop_board_insert_stmts: session.execute(stmt)
    except Exception as e:
        print(e)
        return {"success": False}
    
    return {"success": True}
