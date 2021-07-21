import itertools
from pathlib import Path
from toolz import dicttoolz, itertoolz, functoolz, curried


def seq_bytes_to_utf8(mapping):
    """Decodes a byte string to utf-8"""
    b2s = lambda s: s.decode() if isinstance(s, bytes) else s
    byte_2_utf8 = lambda seq: [dicttoolz.valmap(b2s, d) for d in seq]
    return dicttoolz.valmap(byte_2_utf8, mapping)


def seq_get_values_from_dict_keys(dict_keys):
    """Extracts "value" data from "key" column name."""
    pluck_data = lambda seq: sorted(
        list(itertoolz.pluck(dict_keys, seq, default=None)),
        key=itertoolz.last,
        reverse=True,
    )
    return curried.valmap(pluck_data)


def seq_group_by_dict_key(dict_key):
    groupby_dict_key = lambda seq: itertoolz.groupby(dict_key, seq)
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


def pick(seq):
    p = functoolz.partial(lambda k: k in seq)
    return curried.keyfilter(p)


def seq_pick_dict_keys(dict_keys):
    pick_keys = lambda seq: pick(dict_keys)
    return curried.valmap(pick_keys)


def npick(seq, d):
    return dicttoolz.keyfilter(lambda k: k in seq, d)


def chainmap(*maps):
    from collections import ChainMap

    chain_maps = lambda map: ChainMap(itertoolz.first(map))
    return curried.merge_with(chain_maps, *maps)


def seq_grab_dict_keys(dict_keys):
    pick_keys = lambda seq: [npick(dict_keys, d) for d in seq]
    return curried.valmap(pick_keys)


def seq_sort_by_dict_key(dict_key, reverse=True):
    sort_by_key = functoolz.partial(sorted, key=curried.get(dict_key), reverse=reverse)
    return curried.valmap(sort_by_key)


def seq_first_element(mapping):
    return dicttoolz.valmap(itertoolz.first, mapping)


def seq_second_element(mapping):
    return dicttoolz.valmap(itertoolz.second, mapping)


def seq_last_element(mapping):
    return dicttoolz.valmap(itertoolz.last, mapping)


def seq_nth_element(int):
    nth_value = lambda seq: itertoolz.nth(int, seq)
    return curried.valmap(nth_value)


def path_filter_files(mapping):
    isfile = lambda path: path.is_file()
    return dicttoolz.valfilter(isfile, mapping)


def path_filter_row_one_value(value):
    """filter value in first line of file"""
    has_value = lambda path: value in path.open().readline()
    return curried.valfilter(has_value)


def xpath_filter_row_one_value(value):
    """filter value in first line of file"""
    has_value = lambda path: value not in path.open().readline()
    return curried.valfilter(has_value)


def merge_maps(*maps, **kwargs):
    return curried.valmap(*maps, **kwargs)


def keyjoin(
    leftkey="",
    leftseq=None,
    lprefix="",
    rightkey="",
    rightseq=None,
    rprefix="",
    left_default="__no__default__",
    right_default="__no__default__",
):

    # join, keymap, merge
    # consider using merge_with to apply function to values after merge operation
    leftkey = lprefix + leftkey
    leftseq = (dicttoolz.keymap(lambda x: lprefix + x, x) for x in leftseq)

    rightkey = rprefix + rightkey
    rightseq = (dicttoolz.keymap(lambda x: rprefix + x, x) for x in rightseq)
    return itertools.starmap(
        dicttoolz.merge,
        itertoolz.join(
            leftkey=leftkey,
            leftseq=leftseq,
            rightkey=rightkey,
            rightseq=rightseq,
            left_default=left_default,
            right_default=right_default,
        ),
    )
