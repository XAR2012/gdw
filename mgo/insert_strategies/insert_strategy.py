from abc import ABCMeta, abstractmethod
from mgoutils.catalog import catalog

class InsertStrategy():
    __metaclass__ = ABCMeta

    def __init__(self, gdw_transform):
        self.gdw_transform = gdw_transform
        self.target_alias = gdw_transform.target_alias
        self.target_table = self.target_alias.sql_table
        self.col_names = gdw_transform.col_names()
        self.select_sql = gdw_transform.generate_sql()
        self.start, self.end = gdw_transform.start, gdw_transform.end

    @abstractmethod
    def generate_insert():
        raise NotImplementedError("You should implement this!")
