from .insert_strategy import InsertStrategy

class SimpleInsert(InsertStrategy):
    def generate_insert(self):
        return (self
                .target_table
                .insert()
                .from_select(self.col_names, self.select_sql))
