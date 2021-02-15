from sqlalchemy import create_engine, MetaData
from functional import seq

from src.defs.utils import PostgreTable

DATABASE_USER = "postgres"
PASSWORD = "fleek-app-prod1"
DBNAME = "ktest"
PROJECT = 'staging'

conn_str = f"postgres://{DATABASE_USER}:{PASSWORD}@localhost:5431/{DBNAME}"
engine = create_engine(conn_str)
metadata = MetaData(engine, schema=PROJECT)

PRODUCT_INFO_TABLE = PostgreTable("product_info", metadata, autoload=True)
PRODUCT_PRICE_HISTORY_TABLE = PostgreTable("product_price_history", metadata, autoload=True)
PRODUCT_RECS_TABLE = PostgreTable("user_product_recommendations", metadata, autoload=True)
SIMILAR_ITEMS_TABLE = PostgreTable("similar_products_v2", metadata, autoload=True)
TOP_PRODUCTS_TABLE = PostgreTable("top_products", metadata, autoload=True)
USER_EVENTS_TABLE = PostgreTable("user_events", metadata, autoload=True)
USER_FAVES_TABLE = PostgreTable("user_faves", metadata, autoload=True)
USER_BAG_TABLE = PostgreTable("user_bag", metadata, autoload=True)
USER_TRASH_TABLE = PostgreTable("user_trash", metadata, autoload=True)
