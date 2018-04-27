import logging
import yaml
from cmdlineutil import CronJob
import de_common.scriptutil as scriptutil
from de_common.datetimeutil import days_ago
from mgoutils.catalog import GDWCatalog
from sqlalchemy import text
from sqlalchemy.sql import select, case, and_, or_, not_, literal_column
from sqlalchemy.sql.functions import concat, coalesce

__author__ = 'jvalenzuela'
DISPLAY_NAME = scriptutil.get_display_name(__file__)
TOOL_NAME = scriptutil.get_tool_name(DISPLAY_NAME)
_logger = logging.getLogger(TOOL_NAME)

class GDWTransform(CronJob):
    def __init__(self):
        super(GDWTransform, self).__init__()
        self.config = self.props
        self.load_id = self.opts.load_id
        self._catalog = None
        self._transforms = None

    name = TOOL_NAME
    display_name = DISPLAY_NAME

    yesterday_str = days_ago(1, as_string=False)
    options = [
        (('-l', '--load-id'), dict(type=str, dest='load_id', required=True)),
        (('-d', '--dry-run'), dict(type=bool, dest='dry_run', default=False))]

    @property
    def catalog(self):
        if self._catalog is None:
            self._catalog = GDWCatalog(self.config)
        return self._catalog

    @property
    def transforms(self):
        if self._transforms is None:
            with open('metadata/transform/{}.yaml'.format(self.load_id)) as f:
                    self._transforms = yaml.load(f)
        return self._transforms

    @property
    def insert_mode(self):
        return 'append' if self.transforms.get('append', False) else 'truncate'

    @property
    def target_alias(self):
        return self.transforms.get('target_alias', self.opts.load_id)

    def from_aliases(self):
        for t in self.transforms['from']:
            if isinstance(t, dict):
                alias = t.keys()[0]
                join_dict = t.values()[0]
            else:
                alias = t
                join_dict = {}
            yield alias, join_dict

    def from_aliases_names(self):
        return [i[0] for i in self.from_aliases()]

    def target_columns(self):
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
        for alias, join_dict in self.from_aliases():
            try:
                on_clause = self.find_joins(left, alias)
                from_clause = from_clause.join(
                        self.catalog.tables[alias],
                        onclause=text(on_clause),
                        isouter=join_dict.get('how') == 'left')
                left.append(alias)
            except NameError:
                left = [alias]
                from_clause = self.catalog.tables[alias]
        return from_clause

    def generate_select(self):
        result = []
        for column_name, column_expression in self.target_columns():
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
        target_table = self.catalog.tables[self.target_alias]
        sql = target_table.insert().from_select([i[0] for i in self.target_columns()], query)
        return sql

    def _run_impl(self):
        sqls = []
        target_alias = self.target_alias
        engine = self.catalog.engine_from_alias([target_alias] + self.from_aliases_names())

        if self.insert_mode == 'truncate':
            target_table = self.catalog.tables[self.target_alias]
            truncate_sql = 'TRUNCATE TABLE {};'.format(target_table)
            sqls.append(('TRUNCATE', text(truncate_sql)))

        sqls.append(('INSERT', self.generate_sql()))

        for process, sql in sqls:
            if self.opts.dry_run:
                _logger.info("Dry run. {} SQL statement not run:".format(process))
                _logger.info(sql)
            else:
                _logger.info("Executing {} process in database".format(process))
                engine.execute(sql.execution_options(autocommit=True))


if __name__ == '__main__':
    app = GDWTransform()
    app.run()
