from ds_toolz import (
    valmap,
    valfilter,
    keyfilter,
    pluck,
    pick,
    npick,
    partial,
    curried,
    groupby,
    unique,
    first,
    second,
    last,
    nth,
)
from datadict import DataDict


def seq_bytes_to_utf8(mapping):
    """Decodes a byte string to utf-8"""
    b2s = lambda s: s.decode() if isinstance(s, bytes) else s
    byte_2_utf8 = lambda seq: [valmap(b2s, d) for d in seq]
    return valmap(byte_2_utf8, mapping)


def seq_get_values_from_dict_keys(dict_keys):
    """Extracts "value" data from "key" column name."""
    pluck_data = lambda seq: sorted(
        list(pluck(dict_keys, seq, default=None)), key=last, reverse=True
    )
    return curried.valmap(pluck_data)


def seq_group_by_dict_key(dict_key):
    groupby_dict_key = lambda seq: groupby(dict_key, seq)
    return curried.valmap(groupby_dict_key)


def eseq_group_by_dict_key(dict_key, *funcs):
    groupby_dict_key = lambda seq: DataDict(groupby(dict_key, seq)).pipe(*funcs).data
    return curried.valmap(groupby_dict_key)


def seq_filter_by_dict_key(dict_key):
    """Returns sequence"""
    filter_key = lambda seq: [d for d in seq if dict_key in d.keys()]
    return curried.valfilter(filter_key)


def xseq_filter_by_dict_key(dict_key):
    filter_key = lambda seq: [d for d in seq if dict_key not in d.keys()]
    return curried.valfilter(filter_key)


def seq_filter_value_by_dict_key(value, key):
    key_value = lambda seq: [d for d in seq if d[key] == value]
    return curried.valfilter(key_value)


def xseq_filter_value_by_dict_key(value, key):
    key_value = lambda seq: [d for d in seq if d[key] != value]
    return curried.valfilter(key_value)


def seq_pick_dict_keys(dict_keys):
    pick_keys = lambda seq: pick(dict_keys)
    return curried.valmap(pick_keys)


def seq_grab_dict_keys(dict_keys):
    pick_keys = lambda seq: [npick(dict_keys, d) for d in seq]
    return curried.valmap(pick_keys)


def seq_sort_by_dict_key(dict_key, reverse=True):
    sort_by_key = partial(sorted, key=curried.get(dict_key), reverse=reverse)
    return curried.valmap(sort_by_key)


def seq_first_element(mapping):
    return valmap(first, mapping)


def seq_second_element(mapping):
    return valmap(second,mapping)


def seq_last_element(mapping):
    return valmap(last, mapping)


def seq_nth_element(int):
    nth_value = lambda seq: nth(int, seq)
    return curried.valmap(nth_value)

