from pathlib import Path
from toolz import itertoolz, curried
import vaex


transform_path_to_posix = lambda path: path.as_posix()


def path_to_posix():
    return curried.valmap(transform_path_to_posix)


transform_xlsx_to_vaex = lambda path: vaex.from_ascii(path, seperator="\t")


def xlsx_to_vaex():
    return curried.valmap(transform_ascii_to_vaex)


transform_ascii_to_vaex = lambda path: vaex.from_ascii(path, seperator="\t")


def ascii_to_vaex():
    return curried.valmap(transform_ascii_to_vaex)


transform_ascii_to_vaex2 = lambda path: vaex.from_ascii(path)


def ascii_to_vaex2():
    return curried.valmap(transform_ascii_to_vaex2)


transform_vaex_to_list = lambda df: [itertoolz.second(x) for x in df.iterrows()]


def vaex_rows_to_list():
    return curried.valmap(transform_vaex_to_list)


transform_vaex_to_dict = lambda df: df.to_dict()


def vaex_to_dict():
    return curried.valmap(transform_vaex_to_dict)
