from pathlib import Path
from vaex.dataframe import DataFrame, Column
from toolz import dicttoolz, curried, itertoolz

def transform_path_to_posix(mapping):
    as_posix = lambda path: path.as_posix()
    return dicttoolz.valmap(as_posix, mapping)


def transform_ascii_to_vaex(mapping):
    """Transforms an ascii/text, tab seperated file into a vaex dataframe."""
    to_vaex_dframe = lambda path: DataFrame.from_ascii(path, seperator="\t")
    return dicttoolz.valmap(to_vaex_dframe, mapping)


def transform_vaex_to_gen(mapping):
    """Transforms vaex dataframe rows into a generator of python dicts."""
    dframe_to_iterrows = lambda df: (itertoolz.second(x) for x in df.iterrows())
    return dictoolz.valmap(dframe_to_iterrows, mapping)


def transform_vaex_to_dict(mapping):
    to_dict = lambda df: df.to_dict()
    return valmap(to_dict, mapping)