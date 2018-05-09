import logging
import yaml
from cmdlineutil import CronJob
import de_common.scriptutil as scriptutil
from de_common.datetimeutil import days_ago
from mgoutils.catalog import GDWCatalog
from sqlalchemy import text
from sqlalchemy.sql import select, literal_column
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

    yesterday_str = days_ago(1, as_string=False)
    options = [
        (('-t', '--target'), dict(type=str, dest='target', required=True)),
        (('-d', '--dry-run'), dict(type=bool, dest='dry_run', default=False))]

    def _run_impl(self):
        gdw_transform = GDWTransform(self.target_alias, self.config)
        engine = gdw_transform.engine
        sql = gdw_transform.generate_sql()
        col_names = [i[0] for i in gdw_transform.col_names_expressions()]
        insert_sql = (
                gdw_transform.target_table
                .insert()
                .from_select(col_names, sql))

        if self.opts.dry_run:
            _logger.info("Dry run. INSERT SQL statement not run:")
            _logger.info(insert_sql)
        else:
            _logger.info("Executing INSERT process in database")
            engine.execute(insert_sql.execution_options(autocommit=True))


class GDWTransform(CronJob):
    def __init__(self, target_alias, config):
        self.target_alias = target_alias
        self.catalog = GDWCatalog(config)
        self._transforms = None

    @property
    def transforms(self):
        if self._transforms is None:
            with open(
                    'metadata/{}.transform.yaml'
                    .format(self.target_alias)) as f:
                self._transforms = yaml.load(f)
        return self._transforms

    @property
    def target_table(self):
        return self.catalog.tables[self.target_alias]

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
            if len(aliases_in_from) > 1:
                tables = [
                        select([self.catalog.tables[a]]) for a
                        in aliases_in_from]
                select_tables = alias(union_all(*tables), rename_to)
            else:
                table = aliases_in_from[0]
                select_tables = alias(self.catalog.tables[table], rename_to)

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

    def generate_where(self):
        return text(self.transforms['where'])

    def generate_sql(self):
        from_clause = self.generate_from()
        select_clause = self.generate_select()
        where_clause = self.generate_where()
        query = (
                select(select_clause)
                .select_from(from_clause)
                .where(where_clause))
        return query


if __name__ == '__main__':
    app = CronGDWTransform()
    app.run()
