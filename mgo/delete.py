import logging
from cmdlineutil import CronJob
import de_common.scriptutil as scriptutil
from de_common.datetimeutil import days_ago
from mgoutils.catalog import GDWCatalog

__author__ = 'jvalenzuela'
DISPLAY_NAME = scriptutil.get_display_name(__file__)
TOOL_NAME = scriptutil.get_tool_name(DISPLAY_NAME)
_logger = logging.getLogger(TOOL_NAME)


class GDWDelete(CronJob):
    def __init__(self):
        super(GDWDelete, self).__init__()
        self.config = self.props
        self.target_alias = self.opts.target
        self._catalog = None

    name = TOOL_NAME
    display_name = DISPLAY_NAME

    yesterday_str = days_ago(1, as_string=False)
    options = [
        (('-t', '--target'), dict(type=str, dest='target', required=True)),
        (('-d', '--dry-run'), dict(type=bool, dest='dry_run', default=False))]

    @property
    def catalog(self):
        if self._catalog is None:
            self._catalog = GDWCatalog(self.config)
        return self._catalog

    def generate_delete(self):
        alias = self.catalog.aliases[self.target_alias]
        target_table = self.catalog.tables[self.target_alias]

        delete_definition = alias.get('delete', {})
        how = delete_definition.get('how', 'truncate')
        what = delete_definition.get('what', 'all')

        if how == 'truncate':
            if what == 'all':
                return 'TRUNCATE TABLE {};'.format(target_table)

    def _run_impl(self):
        engine = self.catalog.engine_from_alias(self.target_alias)

        delete_sql = self.generate_delete()

        if self.opts.dry_run:
            _logger.info("Dry run. DELETE SQL statement not run:")
            _logger.info(delete_sql)
        else:
            _logger.info("Executing DELETE process in database")
            engine.execute(delete_sql.execution_options(autocommit=True))


if __name__ == '__main__':
    app = GDWDelete()
    app.run()
