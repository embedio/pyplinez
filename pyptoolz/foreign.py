from toolz import dicttoolz, curried, itertoolz
from vaex import from_pandas


def vaex_filter_value_by_column(value, column_name):
    filter_value = lambda df: value in df[column_name].tolist()
    return curried.valfilter(filter_value)


def vaex_get_value_by_column(value, column_name):
    get_value = lambda df: df[df[column_name] == value]
    return curried.valmap(get_value)


def vaex_filter_value_gt(value, column_name):
    greater_than = lambda df: df.filter(df[column_name] > value)
    return curried.valmap(greater_than)


def vaex_filter_value_gte(value, column_name):
    greater_than_inclusive = lambda df: df.filter(df[column_name] >= value)
    return curried.valmap(greater_than_inclusive)


def vaex_filter_value_lt(value, column_name):
    less_than = lambda df: df.filter(df[column_name] < value)
    return curried.valmap(less_than)


def vaex_filter_value_lte(value, column_name):
    less_than_inclusive = lambda df: df.filter(df[column_name] <= value)
    return curried.valmap(less_than_inclusive)


def vaex_filter_value_eq(value, column_name):
    get_value = lambda df: df.filter(df[column_name] == value)
    return curried.valmap(get_value)


def vaex_filter_column(column_name):
    get_column = lambda df: column_name in df.column_names
    return curried.valfilter(get_column)


def vaex_sort_column(column_name):
    """Sort order is high to low"""
    sort_column = lambda df: df.sort(df[column_name], ascending=False)
    return curried.valmap(sort_column)


def xvaex_filter_value_by_column(value, column_name):
    filter_value = lambda df: value not in df[column_name].tolist()
    return curried.valfilter(filter_value)


def xvaex_get_value_by_column(value, column_name):
    get_value = lambda df: df[df[column_name] != value]
    return curried.valmap(get_value)


def xvaex_filter_column(column_name):
    get_column = lambda df: column_name not in df.column_names
    return curried.valfilter(get_column)


def vaex_get_column_names(mapping):
    column_names = lambda df: df.column_names
    return dicttoolz.valmap(column_names, mapping)


def vaex_unique_column_values(column_name):
    unique_column = lambda df: sorted(set(df[column_name].tolist()))
    return curried.valmap(unique_column)


def vaex_transpose_vaex(mapping):
    transpose_vaex = lambda df: from_pandas(df.to_pandas_df().transpose())
    return dicttoolz.valmap(transpose_vaex, mapping)


def vaex_percent_floor(percent, column_name="Data_Value"):
    approx_percent = lambda df: df.percentile_approx(
        df[column_name], percentage=percent
    )
    percent_floor = lambda df: df[df[column_name] >= approx_percent(df)]
    return curried.valmap(percent_floor)


def vaex_percent_ceiling(percent, column_name="Data_Value"):
    approx_percent = lambda df: df.percentile_approx(
        df[column_name], percentage=percent
    )
    percent_ceiling = lambda df: df[df[column_name] <= approx_percent(df)]
    return curried.valmap(percent_ceiling)


def vaex_drop_columns(column_names):
    drop_columns = lambda df: df.drop(column_names)
    return curried.valmap(drop_columns)
