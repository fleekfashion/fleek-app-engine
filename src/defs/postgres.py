from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine import Engine
from functional import seq

from src.defs.utils import PostgreTable

DATABASE_USER = "postgres"
PASSWORD = "fleek-app-prod1"
DBNAME = "ktest"
PROJECT = 'staging'

conn_str = f"postgres://{DATABASE_USER}:{PASSWORD}@localhost:5431/{DBNAME}"
engine: Engine = create_engine(conn_str)
metadata = MetaData(engine, schema=PROJECT)

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