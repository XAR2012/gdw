import logging
import collections
from cmdlineutil import CronJob
import de_common.scriptutil as scriptutil
from mgoutils.catalog import catalog
from mgoutils.sqlutils import compile_sql
from mgoutils.dateutils import DEFAULT_START, DEFAULT_END, filter_date_range, parse_date
from transform import GDWTransform
from delete import GDWDelete
from sqlalchemy import text
from sqlalchemy.sql import select, case, and_, or_, not_, literal_column
from sqlalchemy.sql.expression import union_all, alias
from sqlalchemy.sql.functions import concat, coalesce
from insert_strategies import get_insert_strategy

__author__ = 'jvalenzuela'
DISPLAY_NAME = scriptutil.get_display_name(__file__)
TOOL_NAME = scriptutil.get_tool_name(DISPLAY_NAME)
_logger = logging.getLogger(TOOL_NAME)


class CronGDWLoad(CronJob):
    def __init__(self):
        super(CronGDWLoad, self).__init__()
        self.config = self.props
        self.target_alias = self.opts.target

    name = TOOL_NAME
    display_name = DISPLAY_NAME

    options = [
        (('-t', '--target'), dict(type=str, dest='target', required=True)),
        (('-s', '--start'), dict(type=str, dest='start_datetime', required=False)),
        (('-e', '--end'), dict(type=str, dest='end_datetime', required=False)),
        (('-d', '--dry-run'), dict(type=bool, dest='dry_run', default=False))]

    def _run_impl(self):
        catalog.configure(self.config)
        gdw_load = GDWLoad(
                self.target_alias, self.config,
                self.opts.start_datetime,
                self.opts.end_datetime)
        engine = gdw_load.engine
        for description, sqls in gdw_load.generate_load():
            if not isinstance(sqls, collections.Iterable):
                sqls = [sqls]
            for sql in sqls:
                if self.opts.dry_run:
                    _logger.info("Dry run. {} SQL statement not run:"
                            .format(description))
                    _logger.info(compile_sql(sql, engine))
                else:
                    _logger.info("Executing {} process in database"
                            .format(description))
                    engine.execute(sql.execution_options(autocommit=True))


# delete the data for the specified day
class GDWLoad():
    def __init__(self, target_alias_name, config, start=None, end=None):
        self.target_alias = catalog.aliases[target_alias_name]
        self.config = config
        self.target_table = self.target_alias.sql_table

        self.start = parse_date(start or DEFAULT_START)
        self.end = parse_date(end or DEFAULT_END)

    @property
    def engine(self):
        return catalog.engine_from_alias(self.target_alias.name)

    def generate_insert(self, gdw_transform):
        insert_strategy = get_insert_strategy(gdw_transform)
        return insert_strategy.generate_insert()

    # this function generates a list of sql stagements needed to load target alias
    # 1. delete affected partition/date range. Uses the Delete operator
    # 2. generate the transform as a base for the insert. Uses the Transform operator
    # 3. generate the insert part. The insert will depend on whether we want to update
    #      fields based on a PK, whether is a SCD type field, etc.
    def generate_load(self):

        gdw_delete = GDWDelete(
                self.target_alias.name, self.config,
                self.start,
                self.end)
        delete_sql = gdw_delete.generate_delete()

        gdw_transform = GDWTransform(
                self.target_alias.name, self.config,
                self.start,
                self.end)

        insert_sql = self.generate_insert(gdw_transform)
        return [('DELETE', delete_sql),
                ('INSERT', insert_sql)]


if __name__ == '__main__':
    app = CronGDWLoad()
    app.run()
