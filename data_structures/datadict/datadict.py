from dataclasses import dataclass
from collections import UserDict
from types import MappingProxyType
from ds_toolz import dictoolz, pipe 


@dataclass
class DataDict(UserDict):
    def __init__(self, data):
        self.data = MappingProxyType(data)

    def pipe(self, *funcs):
        return DataDict(pipe(self.data, *funcs))

    def items(self):
        return self.data.items()

    def values(self):
        return self.data.values()

    def keys(self):
        return self.data.keys()


@dataclass
class Enhanced_DataDict(DataDict):
    def __init__(self, data):
        super().__init__(data)

    def pipe(self, *funcs):
        return Enhanced_DataDict(pipe(self.data, *funcs))

    # same as native get except can accept list of indices
    def eget(self, ind, default="__no__default__"):
        return dictoolz.get(ind, self.data, default)

    def itemmap(self, func):
        return Enhanced_DataDict(dictoolz.itemmap(func, self.data))

    def keymap(self, func):
        return Enhanced_DataDict(dictoolz.keymap(func, self.data))

    def valmap(self, func):
        return Enhanced_DataDict(dictoolz.valmap(func, self.data))

    def itemfilter(self, predicate):
        return Enhanced_DataDict(dictoolz.itemfilter(predicate, self.data))

    def keyfilter(self, predicate):
        return Enhanced_DataDict(dictoolz.keyfilter(predicate, self.data))

    def valfilter(self, predicate):
        return Enhanced_DataDict(dictoolz.valfilter(predicate, self.data))

    def merge(self, *dicts, **kwargs):
        return Enhanced_DataDict(dictoolz.merge(self.data, *dicts, **kwargs))

    def merge_with(self, func, *dicts, **kwargs):
        return Enhanced_DataDict(dictoolz.merge_with(func, self.data, *dicts, **kwargs))

    def update_in(self, keys, func, default=None):
        return Enhanced_DataDict(dictoolz.update_in(self.data, keys, func, default))

    def assoc(self, key, value):
        return Enhanced_DataDict(dictoolz.assoc(self.data, key, value))

    def assoc_in(self, keys, value):
        return Enhanced_DataDict(dictoolz.assoc_in(self.data, keys, value))

    def dissoc(self, *keys, **kwargs):
        return Enhanced_DataDict(dictoolz.dissoc(self.data, *keys, **kwargs))

    def pick(self, seq):
        return Enhanced_DataDict(dictoolz.keyfilter(lambda k: k in seq, self.data))


if __name__ == "__main__":
    from pathlib import Path
    from datadict_toolbox.dictoolz import *

    data = {path.stem: path for path in Path("text_data").iterdir()}

    datadict = DataDict(data)
    edatadict = Enhanced_DataDict(data)
