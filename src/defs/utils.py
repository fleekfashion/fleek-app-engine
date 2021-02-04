from functools import partial 
from sqlalchemy import Table
from functional import seq

def get_schema(self):
    return seq(self.c)
def get_columns(self):
    return seq(self.c).map(lambda x: x.name)

Table.get_columns = get_columns
Table.get_schema = get_schema

PostgreTable = Table

