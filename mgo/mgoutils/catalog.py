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


class GDWAlias(dict):
    def __init__(self, name, catalog, alias_yaml=None):
        self.name = name
        self.catalog = catalog
        self._engine = None

        # load the yaml file into this object dictionary
        if alias_yaml:
            alias_yaml = alias_yaml
        else:
            file_path = path.join(
                        METADATA_DIRECTORY,
                        '{}.alias.yaml'.format(name))
            try:
                with open(file_path) as f:
                    alias_yaml = yaml.load(f)
            except IOError:
                alias_yaml = {}

        super(GDWAlias, self).__init__(alias_yaml)

        # create a sqlalchemy Table into the sql_table property
        try:
            area = catalog.areas[self['area']]
        except KeyError:
            area = None
        if area:
            table_name = self['table']
            database = catalog.engines[area['database']]
            self.sql_table = GDWTable(
                    table_name,
                    catalog.metadata,
                    schema=area['schema'],
                    autoload=True,
                    autoload_with=database
                    )
        else:
            self.sql_table = None

    @property
    def date_column(self):
        return self.get('date', {}).get('field')

    def get_engine(self):
        if self._engine is None:
            try:
                area = self.catalog.areas[self['area']]
                self._engine = self.catalog.engines[area['database']]
            except KeyError:
                pass

        return self._engine

    def set_engine(self, engine):
        self._engine = engine

    engine = property(get_engine,set_engine)

class GDWAliasDict(dict):
    def __init__(self, catalog):
        self.catalog = catalog

    def __getitem__(self, alias_name):
        try:
            alias = dict.__getitem__(self, alias_name)
        except KeyError:
            alias = GDWAlias(alias_name, self.catalog)
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
        self.aliases = GDWAliasDict(self)
        self.metadata = sqlalchemy.MetaData()

    def engine_from_alias(self, alias_list):
        engines = set()
        if not isinstance(alias_list, list):
            alias_list = [alias_list]
        for alias_name in alias_list:
            alias = self.aliases[alias_name]
            if alias.engine:
                engines.add(alias.engine)

        if len(engines) > 1:
            raise RuntimeError('All tables should be in the same database')
        return list(engines)[0]

    def stage_file(self, source_system, table_name, date_start, date_end=None):
        file_name = '{}_{}.tsv.gz'.format(table_name.lower(), date_start.strftime('%Y-%m-%d'))
        return path.join(PSA_PATH, source_system, table_name.lower(), file_name)
