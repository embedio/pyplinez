from pyplinez_toolbox import (
    pick,
    path_filter_row_one_value,
    xpath_filter_row_one_value,
    seq_group_by_dict_key,
    seq_extract_data_from_dict_keys,
    eseq_group_by_dict_key,
    dict_sort_by_key,
)


def seq_pipe_groupby_key_apply(key, *funcs):
    return eseq_group_by_dict_key(key, *funcs)


def seq_pipe_key_groupby(dict_key):
    return seq_group_by_dict_key(dict_key)


def seq_pipe_values_from_keys_get(dict_keys):
    return seq_extract_data_from_dict_keys(dict_keys)


def path_pipe_header_value_include(value):
    return path_filter_row_one_value(value)


def path_pipe_header_value_exclude(value):
    return xpath_filter_row_one_value(value)


def dict_pipe_key_sort(key):
    return dict_sort_by_key(dict_key)


def dict_pipe_fromkeys(keys):
    return pick(keys)
