from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import Engine
from functional import seq

from src.defs.utils import PostgreTable
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base, name_for_collection_relationship

DATABASE_USER = "postgres"
PASSWORD = "fleek-app-prod1"
DBNAME = "ktest"
PROJECT = 'staging'

conn_str = f"postgres://{DATABASE_USER}:{PASSWORD}@localhost:5431/{DBNAME}"
engine: Engine = create_engine(conn_str)
metadata = MetaData(engine, schema=PROJECT)
board_metadata = MetaData(engine, schema=PROJECT)

ADVERTISER_PRODUCT_COUNT_TABLE = PostgreTable("advertiser_product_count", metadata, autoload=True)
PRODUCT_INFO_TABLE = PostgreTable("product_info", metadata, autoload=True)
PRODUCT_PRICE_HISTORY_TABLE = PostgreTable("product_price_history", metadata, autoload=True)
PRODUCT_RECS_TABLE = PostgreTable("user_product_recommendations", metadata, autoload=True)
PRODUCT_SIZE_INFO_TABLE = PostgreTable("product_size_info", metadata, autoload=True)
SIMILAR_ITEMS_TABLE = PostgreTable("similar_products_v2", metadata, autoload=True)
TOP_PRODUCTS_TABLE = PostgreTable("top_products", metadata, autoload=True)
USER_EVENTS_TABLE = PostgreTable("user_events", metadata, autoload=True)

BOARD_TABLE = PostgreTable("board", metadata, autoload=True)
BOARD_TYPE_TABLE = PostgreTable("board_type", metadata, autoload=True)
BOARD_PRODUCT_TABLE = PostgreTable("board_product", metadata, autoload=True)
USER_BOARD_TABLE = PostgreTable("user_board", metadata, autoload=True)
REJECTED_BOARD_TABLE = PostgreTable("rejected_board", metadata, autoload=True)

def _name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    if constraint.name:
        return constraint.name.lower()
    # if this didn't work, revert to the default behavior
    return name_for_collection_relationship(base, local_cls, referred_cls, constraint)

## Map tables to objects
Base = automap_base(metadata=metadata)
Base.prepare(name_for_collection_relationship=_name_for_collection_relationship)
ProductInfo = Base.classes.product_info
Board, UserBoard, BoardType, BoardProduct = Base.classes.board, Base.classes.user_board, Base.classes.board_type, Base.classes.board_product