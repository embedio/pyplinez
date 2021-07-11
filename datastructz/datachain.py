import itertools
from dataclasses import dataclass
from dataseq import Enhanced_DataSeq
from datadict import Enhanced_DataDict



@dataclass
class DataChain:
    def __init__(self, enable_nonlocal=False, parent=None):
        self.parent = parent
        self.enable_nonlocal = enable_nonlocal
        self.map = {}
        self.maps = [self.map]
        if parent is not None:
            self.maps.append(parent.maps)

    def new_child(self, enable_nonlocal=None):
        enable_nonlocal = (
            self.enable_nonlocal
            if enable_nonlocal is None
            else self.__class__(enable_nonlocal=enable_nonlocal, parent=self)
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


@dataclass
class Enhanced_DataChain:
    def __init__(self, enable_nonlocal=False, parent=None):
        self.parent = parent
        self.enable_nonlocal = enable_nonlocal
        self.map = {}
        self.maps = Enhanced_DataSeq([self.map])
        if parent is not None:
            self.maps.append(parent.maps)

    def new_child(self, enable_nonlocal=None):
        enable_nonlocal = (
            self.enable_nonlocal
            if enable_nonlocal is None
            else self.__class__(enable_nonlocal=enable_nonlocal, parent=self)
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
