import logging
from cmdlineutil import CronJob
import de_common.scriptutil as scriptutil
from mgoutils.catalog import GDWCatalog
from mgoutils.sqlutils import compile_sql
from mgoutils.dateutils import DEFAULT_START, DEFAULT_END, filter_date_range, parse_date
from sqlalchemy.sql.expression import delete

__author__ = 'jvalenzuela'
DISPLAY_NAME = scriptutil.get_display_name(__file__)
TOOL_NAME = scriptutil.get_tool_name(DISPLAY_NAME)
_logger = logging.getLogger(TOOL_NAME)


class CronGDWDelete(CronJob):
    def __init__(self):
        super(CronGDWDelete, self).__init__()
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
        gdw_delete = GDWDelete(
                self.target_alias, self.config,
                self.opts.start_datetime,
                self.opts.end_datetime)
        engine = catalog.engine_from_alias(self.target_alias)
        delete_sql = gdw_delete.generate_delete()

        if self.opts.dry_run:
            _logger.info("Dry run. DELETE SQL statement not run:")
            _logger.info(compile_sql(delete_sql, engine))
        else:
            _logger.info("Executing DELETE process in database")
            engine.execute(delete_sql.execution_options(autocommit=True))

class GDWDelete():
    def __init__(self, target_alias_name, config, start=None, end=None):
        self.target_alias = catalog.aliases[target_alias_name]

        self.start = parse_date(start or DEFAULT_START)
        self.end = parse_date(end or DEFAULT_END)

    def generate_delete(self):
        target_table = self.target_alias.sql_table

        delete_definition = self.target_alias.get('delete', {})
        how = delete_definition.get('how', 'truncate')
        what = delete_definition.get('what', 'all')

        if how == 'truncate':
            if what == 'all':
                return 'TRUNCATE TABLE {};'.format(target_table)
            elif what == 'partition':
                raise RuntimeError('TODO: No truncate partition developed yet')
            else:
                raise RuntimeError('Truncate must specify date_range or all')

        elif how == 'delete':
            if what == 'all':
                return delete(table)
            elif what == 'date_range':
                date_column_names = self.target_alias.date_columns
                date_fields = [target_table.c[col] for col in date_column_names]
                filter_dates = filter_date_range(date_fields, self.start, self.end)
                return delete(target_table, whereclause=filter_dates)
            else:
                raise RuntimeError('TODO: add more delete options besides all and date_range')


if __name__ == '__main__':
    app = CronGDWDelete()
    app.run()
