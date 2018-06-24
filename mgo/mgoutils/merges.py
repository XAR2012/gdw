import sqlalchemy
from sqlalchemy.sql import select, literal_column, and_
from sqlalchemy.sql.expression import union, union_all, alias
from sqlalchemy import text
from mgoutils.catalog import catalog, STATE_START_COLUMN, STATE_END_COLUMN
from mgoutils.dateutils import filter_date_range
from sqlalchemy.sql.functions import coalesce, func


# return a merge of several tables (union/union all)
def merge_tables(aliases, merge_type, as_alias, *args, **kwargs):
    sql_tables = []
    for alias in aliases:
        sql_tables.append(select([alias.sql_table]).where(alias.where))

    rename_to = as_alias.split('/')[-1]
    if merge_type == 'union all':
        sql = sqlalchemy.alias(union_all(*sql_tables), rename_to)
        state_date_columns = aliases[0].state_date_columns
        is_deleted_clause = aliases[0].is_deleted_column
    elif merge_type == 'union':
        sql = sqlalchemy.alias(union(*sql_tables), rename_to)
        state_date_columns = aliases[0].state_date_columns
        is_deleted_clause = aliases[0].is_deleted_column
    elif merge_type == 'modifications':
        sql, is_deleted_clause = merge_changes(aliases,
                by=kwargs.get('by'),
                rename_to=rename_to,
                start=kwargs.get('start'), end=kwargs.get('end'))
        state_date_columns = [STATE_START_COLUMN, STATE_END_COLUMN]

    return sql, state_date_columns, is_deleted_clause


def merge_changes(aliases, by=None, start=None, end=None, rename_to=None):
    # TODO add code to check inclusive/exclusive ranges
    if isinstance(by, str):
        by = [by]

    # we are trying to get a list of distinct keys (by columns)
    # and dates (which correspond to the state changes)
    # once we have that we can find the different values in each of the source tables
    # for those keys and date ranges
    def keys_and_changes(aliases, by, start, end):
        keys_and_dates = []
        for alias in aliases:
            from_clause = sqlalchemy.alias(alias.sql_table, alias.basename)
            where_clause = filter_date_range(
                    from_clause.c[alias.modified_date_column],
                    start, end)
            if alias.where:
                where_clause = and_(alias.where, where_clause)

            by_columns = [
                    literal_column('{}'.format(c)).label(c)
                    for c in by]

            # one row for state start and one for end
            for state_date_column in alias.state_date_columns:
                keys_and_dates.append(
                        select(
                            by_columns + 
                            [literal_column(state_date_column)
                                .label(STATE_START_COLUMN)])
                        .select_from(from_clause)
                        .where(where_clause))
        return union(*keys_and_dates)

    keys_and_dts_start = sqlalchemy.alias(
            keys_and_changes(aliases, by, start, end),
            rename_to)

    # add an end state date as the value for the next row with lead function
    from_clause = sqlalchemy.alias(select(
        [keys_and_dts_start.c[c] for c in by] +
        [keys_and_dts_start.c[STATE_START_COLUMN],
        func.lead(literal_column(STATE_START_COLUMN)).over(
            partition_by=by_columns,
            order_by=literal_column(STATE_START_COLUMN))
            .label(STATE_END_COLUMN)],
        from_obj=keys_and_dts_start), rename_to)

    # TODO improve code below
    is_deleted_clause = []
    for alias in aliases:
        on_conditions = [
                text('{rename_to}.{c} = {alias_basename}.{c}'
                    .format(rename_to=rename_to,
                            c=c,
                            alias_basename=alias.basename))
                for c in by]
        if alias.where is not None:
            on_conditions.append(alias.where)
        if alias.state_date_columns:
            start_state_column, end_state_column = [
                    sqlalchemy.alias(alias.sql_table, alias.basename).c[c]
                    for c in alias.state_date_columns]
            on_conditions.append(
                    and_(
                        literal_column('{}.gdw_state_start'.format(rename_to)) >= start_state_column,
                        literal_column('{}.gdw_state_start'.format(rename_to)) < coalesce(end_state_column, '9999-12-31')))
        on_conditions = and_(*on_conditions)

        from_clause = from_clause.join(
                sqlalchemy.alias(alias.sql_table, alias.basename),
                onclause=and_(*on_conditions),
                isouter=True)

        for object_key_column in by:
            is_deleted_clause.append('{}.{} IS NULL'.format(alias.basename, object_key_column))

    is_deleted_clause = ' AND '.join(is_deleted_clause)
    return from_clause, is_deleted_clause


def relationship_with(alias, with_alias):
    for relation_with, relation_dict in alias.get('relationships').items():
        if relation_with == with_alias.name:
            return relation_dict
        return None


# return a list of joins required to join each of the aliases pased as parameters
# it will return a list with one element less than the aliases (as the first table is not joined)
def find_joins(aliases):
    joins = []
    joined = [aliases[0]]
    # starting from the second alias, find joins with all previously checked aliases
    for pos, alias in enumerate(aliases[1:], 1):
        join_dicts = [relationship_with(j, alias) for j in joined]
        if len(join_dicts) > 1:
            raise RuntimeError('Join loop found in the relationships')
        elif len(join_dicts) == 0:
            raise RuntimeError('No relationship found')
        joins.append(join_dicts[0])
        joined.append(alias)
    return joins
