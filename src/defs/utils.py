from functools import partial 

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
