import os

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
DEV_MODE = bool(os.environ.get('DEV_MODE', False))

conn_str = f"postgresql://{DATABASE_USER}:{PASSWORD}@localhost:5431/{DBNAME}"
engine: Engine = create_engine(
        conn_str,
        pool_size=10,
        max_overflow=50,
        poolclass=QueuePool,
        query_cache_size=0
    )
metadata = MetaData(engine, schema=PROJECT,)
sessionMaker = sessionmaker(bind=engine)

def _name_for_collection_relationship(base, local_cls, referred_cls, constraint):
    if constraint.name:
        return constraint.name.lower()
    # if this didn't work, revert to the default behavior
    return name_for_collection_relationship(base, local_cls, referred_cls, constraint)

## Map tables to objects
Base = automap_base(metadata=metadata)
Base.prepare(
    reflect=True,
    name_for_collection_relationship=_name_for_collection_relationship
)

## Product Tables
ProductInfo = Base.classes.product_info
ProductColorOptions = Base.classes.product_color_options
ProductPriceHistory = Base.classes.product_price_history
ProductRecs = Base.classes.user_product_recommendations
ProductLabels= Base.classes.product_labels
ProductSecondaryLabels= Base.classes.product_secondary_labels
ProductSizeInfo = Base.classes.product_size_info
SimilarItems  = Base.classes.similar_products_v2
TopProducts = Base.classes.top_products

## Board Tables
Board = Base.classes.board
BoardProduct = Base.classes.board_product

## Smart Tags
SmartTag = Base.classes.smart_tag
ProductSmartTag = Base.classes.product_smart_tag
BoardSmartTag = Base.classes.board_smart_tag

## User Tables
UserEvents = Base.classes.user_events
UserBoard = Base.classes.user_board
UserFavedBrands = Base.classes.user_faved_brands
UserMutedBrands = Base.classes.user_muted_brands
UserProductFaves = Base.classes.user_product_faves
UserProductBags = Base.classes.user_product_bags
UserProductSeens = Base.classes.user_product_seens

## Order Tables
Advertiser = Base.classes.advertiser
AdvertiserTopSmartTag = Base.classes.advertiser_top_smart_tag 
Order = Base.classes.order
OrderProduct = Base.classes.order_product

## User Profile
UserProfile = Base.classes.user_profile

## Misc
AdvertiserProductCount = Base.classes.advertiser_product_count
RejectedBoardSmartTagPopup = Base.classes.rejected_board_smart_tag_popup
IP_BOARD = Base.classes.ip_board
