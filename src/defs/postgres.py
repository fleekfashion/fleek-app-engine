from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import Engine
from src.defs.utils import PostgreTable
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base

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
USER_FAVED_BRANDS_TABLE = PostgreTable("user_faved_brands", metadata, autoload=True)
USER_MUTED_BRANDS_TABLE = PostgreTable("user_muted_brands", metadata, autoload=True)

USER_PRODUCT_FAVES_TABLE = PostgreTable("user_product_faves", metadata, autoload=True)
USER_PRODUCT_BAGS_TABLE = PostgreTable("user_product_bags", metadata, autoload=True)
USER_PRODUCT_SEENS_TABLE = PostgreTable("user_product_seens", metadata, autoload=True)

BOARD_INFO_TABLE = PostgreTable("board_info", board_metadata, autoload=True)
BOARD_TYPE_TABLE = PostgreTable("board_type", board_metadata, autoload=True)
BOARD_PRODUCTS_TABLE = PostgreTable("board_products", board_metadata, autoload=True)
USER_BOARDS_TABLE = PostgreTable("user_boards", board_metadata, autoload=True)
REJECTED_BOARDS_TABLE = PostgreTable("rejected_boards", board_metadata, autoload=True)

## Map tables to objects
Base = automap_base(metadata=board_metadata)
Base.prepare()
BoardInfo = Base.classes.board_info

def load_session():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session