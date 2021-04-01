from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from src.defs import postgres as p

@contextmanager
def session_scope():
    """Provide a transactional scope for series of operations"""
    session = load_session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise Exception('PostgreSQL query failed.')
    finally:
        session.close()

def load_session() -> Session:
    Session = sessionmaker(bind=p.engine)
    session = Session()
    return session