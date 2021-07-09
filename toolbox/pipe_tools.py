from datadict.datadict_pathlib import (
    path_filter_row_one_value,
    xpath_filter_row_one_value,
)


from datadict.datadict_sequence import (
    seq_group_by_dict_key,
    seq_extract_data_from_dict_keys,
    eseq_group_by_dict_key,
)

from datadict.datadict_dict import dict_sort_by_key


def merge_maps(*maps, **kwargs):
    from toolz import curried
    from curried import valmap

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
    from toolz import join, keymap
    from itertools import starmap

    # consider using merge_with to apply function to values after merge operation
    leftkey = lprefix + leftkey
    leftseq = (keymap(lambda x: lprefix + x, x) for x in leftseq)

    rightkey = rprefix + rightkey
    rightseq = (keymap(lambda x: rprefix + x, x) for x in rightseq)
    return it.starmap(
        merge,
        join(
            leftkey=leftkey,
            leftseq=leftseq,
            rightkey=rightkey,
            rightseq=rightseq,
            left_default=left_default,
            right_default=right_default,
        ),
    )


def pick(seq):
    from toolz import curried, partial
    from curried import keyfilter

    p = partial(lambda k: k in seq)
    return curried.keyfilter(p)


def npick(seq, d):
    from toolz import keyfilter

    return keyfilter(lambda k: k in seq, d)


def chainmap(*maps):
    from collections import ChainMap

    chain_maps = lambda map: ChainMap(first(map))
    return curried.merge_with(chain_maps, *maps)


if __name__ == "__main__":
    import itertools as it
    import more_itertools as mit
    import toolz as tz
    import cytoolz as cz
    import funcy as fun
    import vaex as vx
    from generators import G
