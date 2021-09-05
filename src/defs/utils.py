from functools import partial 
import typing as t

import cachetools
from sqlalchemy import Table
from functional import seq

from src.defs import postgres as p

def get_schema(self):
    return seq(self.c)
def get_columns(self):
    return seq(self.c).map(lambda x: x.name)

Table.get_columns = get_columns
Table.get_schema = get_schema

PostgreTable = Table

def get_relevent_fields(is_swipe_page: bool, is_legacy: bool) -> t.List[str]:
    product_col_names = [
        'product_name', 
        'product_price', 
        'product_sale_price', 
        'advertiser_name', 
        'product_image_url'
    ]
    if is_swipe_page:
        product_col_names.append('product_additional_image_urls')
    if is_legacy:
        product_col_names.append('product_purchase_url')
    return product_col_names

HIDDEN_LABEL_FIELDS = {
    "jeans": "pants",
    "sweatpants": "pants",
    "graphic tee": "shirt",
    "t-shirt": "shirt",
    "blouse": "shirt",
    "leggings": "pants",
    "bikini": "swimwear",
    "romper": "jumpsuit"
}
