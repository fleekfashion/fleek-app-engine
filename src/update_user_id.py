import typing as t

from functional import seq
import sqlalchemy as s

from src.defs import postgres as p
from src.utils.sqlalchemy_utils import session_scope, row_to_dict
from src.utils.hashers import apple_id_to_user_id_hash

def update_table_user_id(table, old_user_id: int, new_user_id: int) -> bool:
    return s.update(table) \
        .where(table.user_id == old_user_id) \
        .values(user_id=new_user_id)

def updateUserId(args: dict) -> dict:
    old_user_id = apple_id_to_user_id_hash(args['old_user_id'])
    new_user_id = apple_id_to_user_id_hash(args['new_user_id'])

    update_stmts = seq(p.Base.classes) \
        .filter(lambda x: 'user_id' in x.__table__.c) \
        .map(lambda x: update_table_user_id(x, old_user_id, new_user_id)) \
        .to_list()

    try:
        with session_scope() as session:
            res = [ session.execute(stmt) for stmt in update_stmts ]
            session.commit()
    except Exception as e:
        print(e)
        return {"success": False}
    return {"success": True}
