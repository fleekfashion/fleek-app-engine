from contextlib import contextmanager
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from src.defs import postgres as p
from copy import copy
from collections import ChainMap
from collections.abc import Iterable

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

def table_row_to_dict(row) -> dict:
    row_copy = copy(row.__dict__)
    row_copy.pop('_sa_instance_state')
    return row_copy

def row_to_dict(row) -> dict:
    print(len(row))
    if isinstance(row,Iterable):
        parsed_rows = [table_row_to_dict(row) for row in row]
        return dict(ChainMap(*parsed_rows))
    else:
        return table_row_to_dict(row)