import logging
import yaml
from cmdlineutil import CronJob
import de_common.scriptutil as scriptutil
from mgoutils.catalog import GDWCatalog, GDWAlias
from mgoutils.sqlutils import compile_sql
from mgoutils.dateutils import DEFAULT_START, DEFAULT_END, filter_date_range, parse_date
from sqlalchemy import text
from sqlalchemy.sql import select, literal_column, and_
from sqlalchemy.sql.expression import union_all, alias

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
        (('-i', '--insert-table'), dict(type=bool, dest='insert_table', default=None))]

    def _run_impl(self):
        gdw_transform = GDWTransform(
                self.target_alias, self.config,
                self.opts.start_datetime,
                self.opts.end_datetime)
        sql = gdw_transform.generate_sql()
        engine = gdw_transform.engine

        if self.opts.insert_table:
            schema, table_name = self.opts.insert_table.split('.')
            target_table = Table(
                    table_name,
                    catalog.metadata,
                    schema=schema,
                    autoload=True,
                    autoload_with=database
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
    def __init__(self, target_alias_name, config=None, start=None, end=None, catalog=None):
        self.catalog = catalog or GDWCatalog(config)
        self.target_alias = self.catalog.aliases[target_alias_name]
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
        return self.catalog.engine_from_alias(self.from_used_alias_names())

    def from_aliases(self):
        def get_alias_dict(alias_yaml):
            if isinstance(alias_yaml, str):
                alias_yaml = {
                        'alias': alias_yaml,
                        'how': 'inner',
                        'as': alias_yaml}
            elif isinstance(alias_yaml, list):
                alias_yaml = {
                        'alias': alias_yaml,
                        'how': 'inner',
                        'as': 'unnamed0'}

            aliases_in_from = alias_yaml['alias']
            how = alias_yaml.get('how', 'inner')

            if isinstance(aliases_in_from, list):
                as_alias = alias_yaml.get('as', 'unnamed0')
            else:
                as_alias = alias_yaml.get('as', aliases_in_from)
                aliases_in_from = [aliases_in_from]

            # in case the alias has a "/"
            rename_to = as_alias.split('/')[-1]
            sql_tables = []
            for table in aliases_in_from:
                sql_table = self.catalog.aliases[table].sql_table
                if sql_table is None:
                    gdw_transform = GDWTransform(
                            table, self.catalog.config,
                            self.start,
                            self.end)
                    sql_table = gdw_transform.generate_sql()
                    self.catalog.aliases[table].sql_table = sql_table
                    self.catalog.aliases[table].engine = gdw_transform.engine

                    #TODO filter the date range
                sql_tables.append(sql_table)
            if len(sql_tables) > 1:
                select_tables = alias(union_all(*sql_tables), rename_to)
            else:
                select_tables = alias(sql_tables[0], rename_to)

            dict_alias = {
                    'alias': aliases_in_from,
                    'select': select_tables,
                    'as': as_alias,
                    'how': how,
                    }
            return dict_alias

        if not isinstance(self.transforms['from'], list):
            self.transforms['from'] = [self.transforms['from']]

        for t in self.transforms['from']:
            result = get_alias_dict(t)
            yield result

    def from_used_alias_names(self):
        return [alias for i in self.from_aliases() for alias in i['alias']]

    def col_names_expressions(self):
        for c in self.transforms['select']:
            column_name = c.keys()[0]
            column_expression = c.values()[0]
            yield column_name, column_expression

    def col_names(self):
        return [i[0] for i in self.col_names_expressions()]

    def relationship_with(self, alias, with_alias):
        alias = self.catalog.aliases[alias]
        for relation_with, relation_dict in alias.get('relationships').items():
            if relation_with == with_alias:
                return relation_dict
            return None

    def find_joins(self, left, right):
        joins = []
        if not isinstance(left, list):
            left = [left]

        for l in left:
            join_dict = self.relationship_with(l, right)
            if join_dict:
                joins.append(join_dict['join_on'])
        if len(joins) > 1:
            raise RuntimeError('Join loop found in the relationships')
        elif len(joins) == 0:
            raise RuntimeError('No relationship found')
        return joins[0]

    def generate_from(self):
        from_aliases = list(self.from_aliases())
        join_dict = from_aliases[0]
        left = [join_dict['as']]
        from_clause = join_dict['select']

        for join_dict in from_aliases[1:]:
            alias = join_dict['as']
            on_clause = self.find_joins(left, alias)
            from_clause = from_clause.join(
                    join_dict['select'],
                    onclause=text(on_clause),
                    isouter=join_dict['how'])
            left.append(alias)
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

        from_aliases = list(self.from_aliases())
        driving_from = from_aliases[0]
        driving_alias_name = driving_from['alias']
        if len(driving_alias_name) == 1:
            driving_alias_name = driving_alias_name[0]
            driving_alias = self.catalog.aliases[driving_alias_name]
            date_field_name = driving_alias.date_column
            if date_field_name:
                date_fields = []
                if isinstance(date_field_name, str):
                    date_field_name = [date_field_name]
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
