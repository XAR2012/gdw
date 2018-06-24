from importlib import import_module

def choose_insert_strategy(load_definition):
    how = load_definition.get('how', 'insert')
    if how == 'insert':
        insert_strategy = 'simple_insert.SimpleInsert'
    elif how == 'dimension':
        type = load_definition.get('type', 'insert')
        if type == 'scd2':
            insert_strategy = 'dimension.SCD2DimensionStrategy'
        elif type == 'daily':
            insert_strategy = 'dimension.DailyDimensionStrategy'

    return insert_strategy

def get_insert_strategy(gdw_transform, *args, **kwargs):
    target_alias = gdw_transform.target_alias
    load_definition = target_alias.get('load', {})

    insert_strategy = choose_insert_strategy(load_definition)
    module_name, class_name = insert_strategy.split('.')
    insert_module = import_module('.' + module_name, package='insert_strategies')
    insert_strategy = getattr(insert_module, class_name)
    instance = insert_strategy(gdw_transform, *args, **kwargs)
    return instance
