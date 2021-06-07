from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.engine import Engine
from sqlalchemy.pool import QueuePool
from functional import seq

from src.defs.utils import PostgreTable
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.automap import automap_base, name_for_collection_relationship

DATABASE_USER = "postgres"
PASSWORD = "fleek-app-prod1"
DBNAME = "ktest"
PROJECT = 'staging'

conn_str = f"postgresql://{DATABASE_USER}:{PASSWORD}@localhost:5431/{DBNAME}"
engine: Engine = create_engine(
        conn_str, 
        pool_size=10, 
        max_overflow=50, 
        poolclass=QueuePool, 
        query_cache_size=0
    )
metadata = MetaData(engine, schema=PROJECT, )
sessionMaker = sessionmaker(bind=engine)

ADVERTISER_PRODUCT_COUNT_TABLE = PostgreTable("advertiser_product_count", metadata, autoload=True)
PRODUCT_INFO_TABLE = PostgreTable("product_info", metadata, autoload=True)
PRODUCT_COLOR_OPTIONS_TABLE = PostgreTable("product_color_options", metadata, autoload=True)
PRODUCT_PRICE_HISTORY_TABLE = PostgreTable("product_price_history", metadata, autoload=True)
PRODUCT_RECS_TABLE = PostgreTable("user_product_recommendations", metadata, autoload=True)
PRODUCT_SIZE_INFO_TABLE = PostgreTable("product_size_info", metadata, autoload=True)
SIMILAR_ITEMS_TABLE = PostgreTable("similar_products_v2", metadata, autoload=True)
TOP_PRODUCTS_TABLE = PostgreTable("top_products", metadata, autoload=True)

BOARD_TABLE = PostgreTable("board", metadata, autoload=True)
BOARD_TYPE_TABLE = PostgreTable("board_type", metadata, autoload=True)
BOARD_PRODUCT_TABLE = PostgreTable("board_product", metadata, autoload=True)
USER_BOARD_TABLE = PostgreTable("user_board", metadata, autoload=True)
REJECTED_BOARD_TABLE = PostgreTable("rejected_board", metadata, autoload=True)

USER_EVENTS_TABLE = PostgreTable("user_events", metadata, autoload=True)
USER_PRODUCT_FAVES_TABLE = PostgreTable("user_product_faves", metadata, autoload=True)
USER_PRODUCT_BAGS_TABLE = PostgreTable("user_product_bags", metadata, autoload=True)
USER_PRODUCT_SEENS_TABLE = PostgreTable("user_product_seens", metadata, autoload=True)
USER_FAVED_BRANDS_TABLE = PostgreTable("user_faved_brands", metadata, autoload=True)
USER_MUTED_BRANDS_TABLE = PostgreTable("user_muted_brands", metadata, autoload=True)

ADVERTISER_TABLE = PostgreTable("advertiser", metadata, autoload=True)
ORDER_TABLE = PostgreTable("order", metadata, autoload=True)
ORDER_PRODUCT_TABLE = PostgreTable("order_product", metadata, autoload=True)

def _name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    if constraint.name:
        return constraint.name.lower()
    # if this didn't work, revert to the default behavior
    return name_for_collection_relationship(base, local_cls, referred_cls, constraint)

## Map tables to objects
Base = automap_base(metadata=metadata)
Base.prepare(name_for_collection_relationship=_name_for_collection_relationship)

## Product Tables
ProductInfo = Base.classes.product_info
ProductColorOptions = Base.classes.product_color_options
ProductPriceHistory = Base.classes.product_price_history
ProductRecs = Base.classes.user_product_recommendations
ProductSizeInfo = Base.classes.product_size_info
SimilarItems  = Base.classes.similar_products_v2
TopProducts = Base.classes.top_products

## Board Tables
Board = Base.classes.board
BoardProduct = Base.classes.board_product
BoardType = Base.classes.board_type

## User Tables
UserEvents = Base.classes.user_events
UserBoard = Base.classes.user_board
UserFavedBrands = Base.classes.user_faved_brands
UserMutedBrands = Base.classes.user_muted_brands

## Order Tables
Advertiser = Base.classes.advertiser
Order = Base.classes.order
OrderProduct = Base.classes.order_product

## Misc
AdvertiserProductCount = Base.classes.advertiser_product_count
