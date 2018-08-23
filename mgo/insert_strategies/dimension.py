from .insert_strategy import InsertStrategy
from sqlalchemy.sql import select, literal_column, and_
from sqlalchemy.sql.functions import coalesce, func
from sqlalchemy import exists
import sqlalchemy
from datetime import timedelta
from mgoutils.dateutils import filter_date_range
from mgoutils.catalog import GDWTable, catalog, STATE_START_COLUMN, STATE_END_COLUMN

class DimensionStrategy(InsertStrategy):
    @property
    def columns(self):
        return self.target_table.column_names

    @property
    def stage_col_names(self):
        return self.col_names + ['gdw_state_dts_range']

    @property
    def dim_col_names(self):
        return self.target_table.column_names

    @property
    def select_sql_columns(self):
        # select only the columns I'm going to insert into the dimension
        return select([ self.select_sql.c[col] for col in self.columns])

    @property
    def object_key_columns(self):
        object_key_columns = self.target_alias['load']['object_key']
        if isinstance(object_key_columns, str):
            object_key_columns = [object_key_columns]
        return object_key_columns

    @property
    def priority_order(self):
        priority_columns = self.target_alias['load']['priority']
        if isinstance(priority_columns, str):
            priority_columns = [priority_columns]
        priority_which = self.target_alias['load'].get('priority_which', 'max')
        if isinstance(priority_which, str):
            priority_which = [priority_which]

        order_by = []
        for c, which in zip(priority_columns, priority_which):
            if which == 'max':
                order_by.append(literal_column(c).desc())
            else:
                order_by.append(literal_column(c))
        order_by.append(literal_column(STATE_START_COLUMN).desc())
        order_by.append(literal_column(STATE_END_COLUMN).desc())
        return order_by


class SCD2DimensionStrategy(DimensionStrategy):
    def generate_insert(self):
        result = []
        # first insert fields for which we have no change record
        staging_alias = catalog.aliases[self.target_alias['load']['staging_alias']]

        # create a subselect from the transform query and add column
        # gdw_state_dts_range = all timestamp ranges for which this row is valid
        select_sql = sqlalchemy.alias(self.select_sql, 'source_transform')
        select_sql_columns = [select_sql.corresponding_column(c) for c in self.select_sql.c]
        gdw_state_dts_range = func.prioritize_ranges(
                func.array_agg(
                    func.tsrange(
                        literal_column('gdw_state_start'),
                        literal_column('gdw_state_end'))
                ).over(partition_by=[literal_column(c) for c in self.object_key_columns],
                       order_by=self.priority_order
                )).label('gdw_state_dts_range')
        select_sql_columns.append(gdw_state_dts_range)

        result.append(staging_alias.sql_table
                .insert()
                .from_select(
                    self.stage_col_names,
                    select(select_sql_columns))
                )

        # select all from existing dimension. look up the object key in stage and remove from
        # gdw_state_dts_range that already is calculated on stage
        select_sql = sqlalchemy.alias(self.select_sql, 'source_transform')
        select_sql_columns = [select_sql.corresponding_column(c) for c in self.select_sql.c]
        gdw_state_dts_range = func.prioritize_ranges(
                func.array_agg(
                    func.tsrange(
                        literal_column('gdw_state_start'),
                        literal_column('gdw_state_end'))
                ).over(partition_by=[literal_column(c) for c in self.object_key_columns],
                       order_by=self.priority_order
                )).label('gdw_state_dts_range')
        select_sql_columns.append(gdw_state_dts_range)

        result.append(staging_alias.sql_table
                .insert()
                .from_select(
                    self.stage_col_names,
                    select(select_sql_columns))
                )
        return result
