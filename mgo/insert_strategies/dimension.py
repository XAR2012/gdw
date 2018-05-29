from .insert_strategy import InsertStrategy
from sqlalchemy.sql import select, and_
from sqlalchemy import exists
from datetime import timedelta
from mgoutils.dateutils import filter_date_range
from mgoutils.catalog import GDWTable

class DimensionStrategy(InsertStrategy):
    @property
    def columns(self):
        return self.target_table.column_names

    @property
    def select_sql_columns(self):
        # select only the columns I'm going to insert into the dimension
        return select([ self.select_sql.c[col] for col in self.columns])

    @property
    def key_column(self):
        return self.target_alias['load']['keep']['key']


class DailyDimensionStrategy(DimensionStrategy):
    def generate_insert(self):
        # first insert fields for which we have no change record
        yesterday = self.start - timedelta(days=1)
        dim_date_column = self.target_table.c[self.target_alias.date_column]
        key_column_name = self.key_column
        from IPython.core.debugger import Tracer; Tracer()()
        # select_sql = self.target_table.join(
        #         self.select_sql,
        #         onclause=and_(
        #             self.select_sql.c[key_column_name] == self.target_table.c[key_column_name],
        #             self.select_sql.c[key_column_name] == self.target_table.c[key_column_name],
        #             )
        return select_sql
        return (self.target_table.insert()
                .from_select(self.columns, self.select_sql_columns))
