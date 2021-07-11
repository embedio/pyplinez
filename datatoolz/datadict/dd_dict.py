from . import curried, dictoolz, functoolz


def dict_filter_by_key(key):
    filter_column = lambda d: dictoolz.keyfilter(lambda x: x == key, d)
    return curried.valfilter(filter_column)


def dict_get_key(key):
    return curried.valmap(curried.get(key))


def dict_sort_by_key(key, reverse=True):
    sort_column = functoolz.partial(sorted, key=curried.get(key), reverse=reverse)
    sorted_seq = lambda seq: dictoolz.valmap(sort_column, seq)
    return curried.valmap(sorted_seq)
