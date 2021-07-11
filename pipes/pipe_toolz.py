# path_filter_row_one_value,
# xpath_filter_row_one_value

# seq_group_by_dict_key,
# seq_extract_data_from_dict_keys,
# eseq_group_by_dict_key

# dict_sort_by_key


import itertools
from toolz import curried, itertoolz, dictoolz, functoolz
from datatoolz.datadict import dd_path, dd_seq, dd_dict


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
    leftseq = (dictoolz.keymap(lambda x: lprefix + x, x) for x in leftseq)

    rightkey = rprefix + rightkey
    rightseq = (dictoolz.keymap(lambda x: rprefix + x, x) for x in rightseq)
    return itertools.starmap(
        dictoolz.merge,
        itertoolz.join(
            leftkey=leftkey,
            leftseq=leftseq,
            rightkey=rightkey,
            rightseq=rightseq,
            left_default=left_default,
            right_default=right_default,
        ),
    )


def pick(seq):
    p = functoolz.partial(lambda k: k in seq)
    return curried.keyfilter(p)


def npick(seq, d):
    return dictoolz.keyfilter(lambda k: k in seq, d)


def chainmap(*maps):
    from collections import ChainMap

    chain_maps = lambda map: ChainMap(itertoolz.first(map))
    return curried.merge_with(chain_maps, *maps)
