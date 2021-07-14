from toolz import dicttoolz, curried, itertoolz
from vaex import DataFrame, Column


def vaex_filter_value_by_column(value, column_name):
    get_value = lambda df: df[df[column_name] == value]
    return curried.valmap(get_value)


def vaex_filter_by_column(column_name):
    get_column = lambda df: column_name in df.column_names
    return curried.valfilter(get_column)


def xvaex_filter_by_column(column_name):
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
