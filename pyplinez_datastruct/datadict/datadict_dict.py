from datadict_toolbox.curried import valfilter, valmap

def dict_filter_by_key(key):
    filter_column = lambda d: keyfilter(lambda x: x == key, d)
    return valfilter(filter_column)


def dict_get_key(key):
    return valmap(curried.get(key))


def dict_sort_by_key(key, reverse=True):
    sort_column = partial(sorted, key=curried.get(key), reverse=reverse)
    sorted_seq = lambda seq: valmap(sort_column, seq)
    return valmap(sorted_seq)


if __name__ == "__main__":
    from pathlib import Path

    from datadict_toolbox import *
    from datadict_pathlib import *

    data = {path.stem: path for path in Path("text_data").iterdir()}
