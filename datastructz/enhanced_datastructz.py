import itertools
from dataclasses import dataclass
from collections import UserDict, UserList
from types import MappingProxyType
from toolz import dicttoolz, functoolz, itertoolz, recipes


@dataclass
class Enhanced_DataSeq(UserList):
    def __init__(self, data=None):
        self.data = data if data != None else []

    def pipe(self, *funcs):
        return Enhanced_DataSeq(functoolz.pipe(self.data, *funcs))

    @property
    def count_all(self):
        return Enhanced_DataSeq(itertoolz.count(self.data))

    @property
    def peek(self):
        return Enhanced_DataSeq(itertoolz.peek(self.data))

    @property
    def first(self):
        return Enhanced_DataSeq(itertoolz.first(self.data))

    @property
    def second(self):
        return Enhanced_DataSeq(itertoolz.second(self.data))

    @property
    def last(self):
        return Enhanced_DataSeq(itertoolz.last(self.data))

    def do(self, func):
        return DataSeq(functoolz.do(func, self.data))

    def remove(self, predicate):
        return Enhanced_DataSeq(itertoolz.remove(predicate, self.data))

    def accumulate(self, binop, initial="__no__default__"):
        return Enhanced_DataSeq(itertoolz.accumulate(binop, self.data, initial))

    def groupby(self, key):
        return Enhanced_DataSeq(itertoolz.groupby(key, self.data))

    def interleave(self, seqs):
        return Enhanced_DataSeq(itertoolz.interleave(self.data, seqs))

    def isdistinct(self, seq):
        return Enhanced_DataSeq(itertoolz.isdistinct(self.data))

    def take(self, n):
        return Enhanced_DataSeq(itertoolz.take(n, self.data))

    def drop(self, n):
        return Enhanced_DataSeq(itertoolz.drop(n, self.data))

    def take_nth(self, n):
        return Enhanced_DataSeq(itertoolz.take_nth(n, self.data))

    def nth(self, n):
        return Enhanced_DataSeq(itertoolz.nth(n, self.data))

    def get(self, ind, default="__no__default__"):
        return Enhanced_DataSeq(itertoolz.get(ind, self.data, default))

    def cons(self, el):
        return Enhanced_DataSeq(itertoolz.cons(el, self.data))

    def interpose(self, el):
        return Enhanced_DataSeq(itertoolz.interpose(el, self.data))

    def sliding_window(self, n):
        return Enhanced_DataSeq(itertoolz.sliding_window(n, self.data))

    def partition(self, n, pad="__no__pad__"):
        return Enhanced_DataSeq(itertoolz.partition(n, self.data, pad))

    def partition_all(self, n):
        return Enhanced_DataSeq(itertoolz.partition_all(n, self.data))

    def pluck(self, ind, default="__no__default__"):
        return Enhanced_DataSeq(itertoolz.pluck(ind, self.data, default))

    def tail(self, n):
        return Enhanced_DataSeq(itertoolz.tail(n, self.data))

    def topk(self, k, key=None):
        return Enhanced_DataSeq(itertoolz.topk(k, self.data, key))

    def peekn(self, n):
        return Enhanced_DataSeq(itertoolz.peekn(n, self.data))

    def random_sample(self, prob, random_state=None):
        return Enhanced_DataSeq(itertoolz.random_sample(prob, self.data, random_state))

    def countby(self, key):
        return Enhanced_DataSeq(recipes.countby(key, self.data))

    def partitionby(self, func):
        return Enhanced_DataSeq(recipes.partitionby(func, self.data))

    def compact(self, func=None):
        from toolz import filter as _filter

        return Enhanced_DataSeq(_filter(func, self.data))

    def clear(self):
        return []

    def sort(self):
        return self.data.copy().sort()

    def remove(self, value):
        return self.data.copy().remove(value)

    def pop(self, index=-1):
        return self.data.copy().pop(index)


@dataclass
class Enhanced_DataDict(UserDict):
    def __init__(self, data):
        self.data = data

    def pipe(self, *funcs):
        return Enhanced_DataDict(functoolz.pipe(self.data, *funcs))

    def iget(self, ind, default="__no__default__"):
        return itertoolz.get(ind, self.data, default)

    def itemmap(self, func):
        return Enhanced_DataDict(dicttoolz.itemmap(func, self.data))

    def keymap(self, func):
        return Enhanced_DataDict(dicttoolz.keymap(func, self.data))

    def valmap(self, func):
        return Enhanced_DataDict(dicttoolz.valmap(func, self.data))

    def itemfilter(self, predicate):
        return Enhanced_DataDict(dicttoolz.itemfilter(predicate, self.data))

    def keyfilter(self, predicate):
        return Enhanced_DataDict(dicttoolz.keyfilter(predicate, self.data))

    def valfilter(self, predicate):
        return Enhanced_DataDict(dicttoolz.valfilter(predicate, self.data))

    def merge(self, *dicts, **kwargs):
        return Enhanced_DataDict(dicttoolz.merge(self.data, *dicts, **kwargs))

    def merge_with(self, func, *dicts, **kwargs):
        return Enhanced_DataDict(
            dicttoolz.merge_with(func, self.data, *dicts, **kwargs)
        )

    def update_in(self, keys, func, default=None):
        return Enhanced_DataDict(dicttoolz.update_in(self.data, keys, func, default))

    def assoc(self, key, value):
        return Enhanced_DataDict(dicttoolz.assoc(self.data, key, value))

    def assoc_in(self, keys, value):
        return Enhanced_DataDict(dicttoolz.assoc_in(self.data, keys, value))

    def dissoc(self, *keys, **kwargs):
        return Enhanced_DataDict(dicttoolz.dissoc(self.data, *keys, **kwargs))

    def pick(self, seq):
        return Enhanced_DataDict(dicttoolz.keyfilter(lambda k: k in seq, self.data))

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def items(self):
        return self.data.items()

    def clear(self):
        return {}

    def pop(self, key):
        return self.data.copy().pop(key)

    def popitem(self, key, value):
        return self.data.copy().popitem(key, value)

    def update(self, other):
        return self.data.copy().update(other)


@dataclass
class Enhanced_DataChain(Enhanced_DataDict):
    def __init__(self, data, enable_nonlocal=False, parent=None):
        self.parent = parent
        self.enable_nonlocal = enable_nonlocal
        self.data = data
        self.maps = Enhanced_DataSeq([self.data])
        if parent is not None:
            self.maps += parent.maps

    def new_child(self, data, enable_nonlocal=None):
        enable_nonlocal = (
            self.enable_nonlocal if enable_nonlocal is None else enable_nonlocal
        )
        return self.__class__(data=data, enable_nonlocal=enable_nonlocal, parent=self)

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
        self.data[key] = value

    def __len__(self, len=len, sum=sum, map=map):
        return sum(map(len, self.maps))

    def __iter__(self, chain_from_iterable=itertools.chain.from_iterable):
        return chain_from_iterable(self.maps)

    def __contains__(self, key, any=any):
        return any(key in m for m in self.maps)

    def __repr__(self, repr=repr):
        return " -> ".join(map(repr, self.maps))


if __name__ == "__main__":
    import toolz as tz
