from contextlib import contextmanager
from src.defs.postgres import load_session

@contextmanager
def session_scope():
    """Provide a transactional scope for series of operations"""
    session = load_session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()