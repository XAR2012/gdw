from os import path
import yaml
from fido.common.db import get_orm_engine
import sqlalchemy
from sqlalchemy import sql
from sqlalchemy.schema import Table

METADATA_DIRECTORY = 'metadata'
PSA_PATH = 'psa'

class GDWTable(Table):
    @property
    def column_names(self):
        try:
            return self._column_names
        except AttributeError:
            self._column_names = []
            for c in self.columns:
                self._column_names.append(c.name)
            return self._column_names


class GDWTableDict(dict):
    def __init__(self, engines, aliases, areas):
        self.engines = engines
        self.aliases = aliases
        self.areas = areas
        self.metadata = sqlalchemy.MetaData()

    def __getitem__(self, alias_name):
        try:
            table = dict.__getitem__(self, alias_name)
        except KeyError:
            alias = self.aliases[alias_name]
            area = self.areas[alias['area']]
            database = self.engines[area['database']]
            table = GDWTable(
                        alias['table'],
                        self.metadata,
                        schema=area['schema'],
                        autoload=True,
                        autoload_with=database
                    )
            dict.__setitem__(self, alias_name, table)
        return table


class GDWAliasDict(dict):
    def __init__(self):
        self.metadata_directory = METADATA_DIRECTORY

    def __getitem__(self, alias_name):
        try:
            alias = dict.__getitem__(self, alias_name)
        except KeyError:
            with open(path.join(
                        self.metadata_directory,
                        '{}.alias.yaml'.format(alias_name))) as f:
                alias = yaml.load(f)
            dict.__setitem__(self, alias_name, alias)
        return alias


class GDWCatalog():
    def __init__(self, config):
        self.config = config
        with open('metadata/areas.yaml') as f:
            self.areas = yaml.load(f)
        self.engines = {
                'bloodmoondb': get_orm_engine(database='eravana_db', config=self.config)
                }
        self.aliases = GDWAliasDict()
        self.tables = GDWTableDict(self.engines, self.aliases, self.areas)

    def engine_from_alias(self, alias_list):
        engines = set()
        if not isinstance(alias_list, list):
            alias_list = [alias_list]
        for alias_name in alias_list:
            alias = self.aliases[alias_name]
            area = self.areas[alias['area']]
            engine = self.engines[area['database']]
            engines.add(engine)

        if len(engines) > 1:
            raise RuntimeError('All tables should be in the same database')
        return engine

    def stage_file(self, source_system, table_name, date_start, date_end=None):
        file_name = '{}_{}.tsv.gz'.format(table_name.lower(), date_start.strftime('%Y-%m-%d'))
        return path.join(PSA_PATH, source_system, table_name.lower(), file_name)
