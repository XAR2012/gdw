import datetime
from de_common.datetimeutil import days_ago, parse_date_string, date_range
from sqlalchemy.sql import or_
import collections

DEFAULT_START = days_ago(1)
DEFAULT_END = parse_date_string(days_ago(1)) + datetime.timedelta(days=1, microseconds=-1)


def parse_date(dt):
    try:
        return parse_date_string(dt)
    except TypeError:
        return dt # it must already be a datetime


def range_days(start, end):
    for n in range((end - start).days + 1):
        yield start + timedelta(n)


def filter_date_range(table_columns, start_date=None, end_date=None):
    if not isinstance(table_columns, collections.Iterable):
        table_columns = [table_columns]

    filters = []
    for c in table_columns:
        # TODO: depending on type of column format the start and end differently
        if 'DATE' in str(c.type) or 1==1:
            start = start_date.strftime('%Y-%m-%d')
            end = end_date.strftime('%Y-%m-%d')

        if start and end:
            if start == end:
                filters.append(c == start)
            else:
                filters.append(c.between(start, end))
        elif start:
            filters.append(c >= start)
        elif end:
            filters.append(c <= end)
        else:
            filters.append(None)

    return or_(*filters)
