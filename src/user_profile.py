from src.utils.sqlalchemy_utils import session_scope
from sqlalchemy.dialects.postgresql import insert
from src.utils import hashers
import src.defs.postgres as p

def upsert_user_profile_info(args: dict) -> dict:
    args['user_id'] = hashers.apple_id_to_user_id_hash(args['user_id'])
    
    upsert_user_profile_stmt = insert(p.UserProfile) \
        .values(**args) \
        .on_conflict_do_update(
            index_elements=['user_id'],
            set_=args
        )
    
    try:
        with session_scope() as session:
            session.execute(upsert_user_profile_stmt)
            session.commit()
    except Exception as e:
        print(e)
        return {
            "success": False
        }

    return {
        "success": True
    }