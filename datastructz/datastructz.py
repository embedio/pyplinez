import itertools
from dataclasses import dataclass
from collections import UserDict, UserList
from types import MappingProxyType
from toolz import dicttoolz, functoolz, itertoolz


@dataclass
class DataSeq(UserList):
    def __init__(self, data=None):
        self.data = data if data != None else []

    def pipe(self, *funcs):
        return DataSeq(functoolz.pipe(self.data, *funcs))


@dataclass
class RO_DataDict(UserDict):
    def __init__(self, data):
        self.data = MappingProxyType(data)

    def pipe(self, *funcs):
        return DataDict(functoolz.pipe(self.data, *funcs))

    def items(self):
        return self.data.items()

    def values(self):
        return self.data.values()

    def keys(self):
        return self.data.keys()


@dataclass
class DataDict(UserDict):
    def __init__(self, data):
        self.data = data

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def items(self):
        return self.data.items()

    def pipe(self, *funcs):
        return Chain_DataDict(pipe(self.data, *funcs))

    def clear(self):
        return {}

    def pop(self, key):
        return self.data.copy().pop(key)

    def popitem(self, key, value):
        return self.data.copy().popitem(key, value)

    def update(self, other):
        return self.data.copy().update(other)


@dataclass
class DataChain(DataDict):
    def __init__(self, enable_nonlocal=False, parent=None):
        self.parent = parent
        self.enable_nonlocal = enable_nonlocal
        self.data = data
        self.maps = [self.map]
        if parent is not None:
            self.maps.append(parent.maps)

    def new_child(self, data, enable_nonlocal=None):
        enable_nonlocal = (
            self.enable_nonlocal
            if enable_nonlocal is None
            else self.__class__(data=data, enable_nonlocal=enable_nonlocal, parent=self)
        )

    @property
    def root(self):
        return self if self.parent is None else self.parent.root

    def __getitem__(self, key):
        for m in self.maps:
            if key in m:
                break
        return m[key]

    def __setitem__(self, key, value):
        if self.enable_nonlocal:
            for m in self.maps:
                if key in m:
                    m[key] = value
                    return
        self.map[key] = value

    def __len__(self, len=len, sum=sum, map=map):
        return sum(map(len, self.maps))

    def __iter__(self, chain_from_iterable=itertools.chain.from_iterable):
        return chain_from_iterable(self.maps)

    def __contains__(self, key, any=any):
        return any(key in m for m in self.maps)

    def __repr__(self, repr=repr):
        return " -> ".join(map(repr, self.maps))
