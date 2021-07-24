from pathlib import Path
from toolz import itertoolz, curried
import vaex


transform_path_to_posix = lambda path: Path(path).as_posix()


def path_to_posix(path):
    return curried.valmap(transform_path_to_posix)


transform_ascii_to_vaex = lambda path: vaex.from_ascii(path, seperator="\t")


def asscii_to_vaex(path):
    return curried.valmap(transform_ascii_to_vaex)


transform_vaex_to_gen = lambda df: (itertoolz.second(x) for x in df.iterrows())


def vaex_to_gen(self, df):
    return curried.valmap(transform_vaex_to_gen)


transform_vaex_to_dict = lambda df: df.to_dict()


def vaex_to_dict(self, df):
    return curried.valmap(transform_vaex_to_dict)
