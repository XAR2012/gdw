import logging
import yaml
from cmdlineutil import CronJob
import de_common.scriptutil as scriptutil
from mgoutils.catalog import GDWTable, catalog
from mgoutils.merges import find_joins, merge_tables
from mgoutils.sqlutils import compile_sql
from mgoutils.dateutils import DEFAULT_START, DEFAULT_END, filter_date_range, parse_date
from sqlalchemy import text
from sqlalchemy.sql import select, literal_column, and_
import sqlalchemy

__author__ = 'jvalenzuela'
DISPLAY_NAME = scriptutil.get_display_name(__file__)
TOOL_NAME = scriptutil.get_tool_name(DISPLAY_NAME)
_logger = logging.getLogger(TOOL_NAME)


class CronGDWTransform(CronJob):
    def __init__(self):
        super(CronGDWTransform, self).__init__()
        self.config = self.props
        self.target_alias = self.opts.target

    name = TOOL_NAME
    display_name = DISPLAY_NAME

    options = [
        (('-t', '--target'), dict(type=str, dest='target', required=True)),
        (('-s', '--start'), dict(type=str, dest='start_datetime', required=False)),
        (('-e', '--end'), dict(type=str, dest='end_datetime', required=False)),
        (('-i', '--insert-table'), dict(type=str, dest='insert_table', default=None))]

    def _run_impl(self):
        catalog.configure(self.config)
        gdw_transform = GDWTransform(
                self.target_alias, self.config,
                self.opts.start_datetime,
                self.opts.end_datetime)
        sql = gdw_transform.generate_sql()
        engine = gdw_transform.engine

        if self.opts.insert_table:
            schema, table_name = self.opts.insert_table.split('.')
            target_table = GDWTable(
                    table_name,
                    catalog.metadata,
                    schema=schema,
                    autoload=True,
                    autoload_with=engine
                    )

            insert_sql = (
                    target_table
                    .insert()
                    .from_select(gdw_transform.col_names(), sql))
            _logger.info("Executing INSERT process in database")
            engine.execute(insert_sql.execution_options(autocommit=True))
        else:
            _logger.info("Dry run. INSERT SQL statement not run:")
            _logger.info(compile_sql(sql, engine))


class GDWTransform(CronJob):
    def __init__(self, target_alias_name, config=None, start=None, end=None):
        self.target_alias = catalog.aliases[target_alias_name]
        self._transforms = None
        self.start = parse_date(start or DEFAULT_START)
        self.end = parse_date(end or DEFAULT_END)

    @property
    def transforms(self):
        if self._transforms is None:
            with open(
                    'metadata/{}.transform.yaml'
                    .format(self.target_alias.name)) as f:
                self._transforms = yaml.load(f)
        return self._transforms

    @property
    def target_table(self):
        return self.target_alias.sql_table

    @property
    def engine(self):
        return catalog.engine_from_alias(self.from_used_alias_names())

    def from_definitions(self):
        def get_alias_dict(single_from):
            # convert this into a dictionary with the expected fields
            if isinstance(single_from, str):
                single_from = {
                        'alias': [single_from],
                        'how': 'inner',
                        'as': single_from}
            elif isinstance(single_from, list):
                single_from = {
                        'alias': single_from,
                        'how': 'inner',
                        'as': single_from[0]}
            elif isinstance(single_from['alias'], str):
                single_from['alias'] = [single_from['alias']]

            alias_names = single_from['alias']

            from IPython.core.debugger import Tracer; Tracer()()
            # if some aliases have no table, try to get the transform sql
            for alias in alias_names:
                if catalog.aliases[alias].sql_table is None:
                    gdw_transform = GDWTransform(
                            alias, catalog.config,
                            self.start,
                            self.end)
                    sql_table = gdw_transform.generate_sql()
                    catalog.aliases[alias].sql_table = sql_table
                    catalog.aliases[alias].engine = gdw_transform.engine
                # TODO filter the date range

            aliases = [catalog.aliases[a] for a in alias_names]

            if len(alias_names) > 1:
                merge_type = single_from.get('merge')
                as_alias, source_sql = merge_tables(aliases,
                        merge_type,
                        by=single_from.get('by'),
                        as_alias=single_from.get('as'),
                        start=self.start, end=self.end)
                where = None
            else:
                as_alias = single_from.get('as', alias_names[0])
                rename_to = as_alias.split('/')[-1]
                source_sql = sqlalchemy.alias(aliases[0].sql_table, rename_to),
                where = aliases[0].where

            dict_alias = {
                    'alias': alias_names,
                    'select': source_sql,
                    'where': where,
                    'as': as_alias,
                    'how': single_from.get('how', 'inner'),
                    }
            return dict_alias

        if not isinstance(self.transforms['from'], list):
            self.transforms['from'] = [self.transforms['from']]

        for t in self.transforms['from']:
            result = get_alias_dict(t)
            yield result

    def from_used_alias_names(self):
        return [alias for i in self.from_definitions() for alias in i['alias']]

    def col_names_expressions(self):
        for c in self.transforms['select']:
            column_name = c.keys()[0]
            column_expression = c.values()[0]
            yield column_name, column_expression

    def col_names(self):
        return [i[0] for i in self.col_names_expressions()]

    def generate_from(self):
        from_definitions = list(self.from_definitions())
        aliases = [catalog.aliases[f['as']] for f in from_definitions]
        joins = [join['join_on'] for join in find_joins(aliases)]
        from_clause = from_definitions[0]['select']

        for from_definition, join in zip(from_definitions[1:], joins):
            alias = catalog.aliases[from_definition['as']]
            on_clause = text(join)
            if alias.where:
                on_clause = and_(on_clause, alias.where)
            from_clause = from_clause.join(
                    from_definition['select'],
                    onclause=on_clause,
                    isouter=from_definition['how'])
        return from_clause

    def generate_select(self):
        result = []
        for column_name, column_expression in self.col_names_expressions():
            result.append(
                    literal_column(column_expression)
                    .label(column_name))
        return result

    def generate_where(self, from_clause):
        filters = []

        transform_where = self.transforms.get('where')
        if transform_where:
            filters.append(text(transform_where))

        from_definitions = list(self.from_definitions())
        driving_from = from_definitions[0]
        driving_alias_name = driving_from['alias']
        if len(driving_alias_name) == 1:
            driving_alias_name = driving_alias_name[0]
            driving_alias = catalog.aliases[driving_alias_name]
            driving_alias = catalog.aliases[driving_alias_name]
            if driving_alias.where is not None:
                filters.append(driving_alias.where)

            date_field_name = driving_alias.date_columns
            if date_field_name:
                date_fields = []
                for col in date_field_name:
                    date_field = driving_from['select'].c[col]
                    date_field = from_clause.corresponding_column(date_field)
                    date_fields.append(date_field)
                filter_dates = filter_date_range(
                        date_fields, self.start, self.end)
                filters.append(filter_dates)

        # filters = [ x for x in filters if x is not None ] # remove empty filters
        if filters:
            return and_(*filters)
        else:
            return None

    def generate_group_by(self):
        try:
            return text(self.transforms['group_by'])
        except:
            return None

    def generate_having(self):
        try:
            return text(self.transforms['having'])
        except:
            return None

    def generate_sql(self):
        select_clause = self.generate_select()
        from_clause = self.generate_from()
        query = (
                select(select_clause)
                .select_from(from_clause))

        where_clause = self.generate_where(from_clause)
        if where_clause is not None:
                query = query.where(where_clause)

        group_by_clause = self.generate_group_by()
        if group_by_clause is not None:
                query = query.group_by(group_by_clause)

        having_clause = self.generate_having()
        if having_clause is not None:
                query = query.having(having_clause)

        return query


if __name__ == '__main__':
    app = CronGDWTransform()
    app.run()
