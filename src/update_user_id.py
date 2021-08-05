import typing as t

from functional import seq
import sqlalchemy as s
from sqlalchemy.sql.expression import literal
from sqlalchemy.sql.dml import Insert, Delete
from sqlalchemy.dialects import postgresql as psql

from src.defs import postgres as p
from src.utils.sqlalchemy_utils import session_scope, row_to_dict
from src.utils.hashers import apple_id_to_user_id_hash
USER_ID_COL = 'user_id'

def insert_new_user_id_data(
    table, 
    old_user_id: int, 
    new_user_id: int
) -> Insert:

    valid_cols = [ col for col in table.__table__.c if col.name != USER_ID_COL ]
    tmp = s.select(
            literal(new_user_id).label(USER_ID_COL),
            *valid_cols
        ) \
        .where(table.user_id == old_user_id) \
        .cte()

    insert_stmt = psql.insert(table) \
        .from_select([ col.name for col in tmp.c ], tmp) \
        .on_conflict_do_nothing()
    return insert_stmt

def delete_old_user_id_data(table, old_user_id: int) -> Delete:
    return s.delete(table) \
        .where(table.user_id == old_user_id)

def updateUserId(args: dict) -> dict:
    old_user_id = apple_id_to_user_id_hash(args['old_user_id'])
    new_user_id = apple_id_to_user_id_hash(args['new_user_id'])

    valid_tables = seq(p.Base.classes) \
        .filter(lambda x: USER_ID_COL in x.__table__.c)

    insert_statements = valid_tables \
        .map(lambda table: insert_new_user_id_data(table, old_user_id, new_user_id)) \
        .to_list()

    delete_statements = valid_tables \
        .map(lambda table: delete_old_user_id_data(table, old_user_id)) \
        .to_list()

    with session_scope() as session:
        [ session.execute(s) for s in insert_statements ]
        #[ session.execute(s) for s in delete_statements ] TEMP DO NOT DELETE FOR V1
        session.commit()
    return {"success": True}
