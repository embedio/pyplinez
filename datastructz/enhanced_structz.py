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
        return self.__class__(functoolz.pipe(self.data, *funcs))

    @property
    def count_all(self):
        return self.__class__(itertoolz.count(self.data))

    @property
    def peek(self):
        return self.__class__(itertoolz.peek(self.data))

    @property
    def first(self):
        return self.__class__(itertoolz.first(self.data))

    @property
    def second(self):
        return self.__class__(itertoolz.second(self.data))

    @property
    def last(self):
        return self.__class__(itertoolz.last(self.data))

    def do(self, func):
        return functoolz.do(func, self.data)

    def remove(self, predicate):
        return self.__class__(itertoolz.remove(predicate, self.data))

    def accumulate(self, binop, initial="__no__default__"):
        return self.__class__(itertoolz.accumulate(binop, self.data, initial))

    def groupby(self, key):
        return self.__class__(itertoolz.groupby(key, self.data))

    def interleave(self, seqs):
        return self.__class__(itertoolz.interleave(self.data, seqs))

    def isdistinct(self, seq):
        return self.__class__(itertoolz.isdistinct(self.data))

    def take(self, n):
        return self.__class__(itertoolz.take(n, self.data))

    def drop(self, n):
        return self.__class__(itertoolz.drop(n, self.data))

    def take_nth(self, n):
        return self.__class__(itertoolz.take_nth(n, self.data))

    def nth(self, n):
        return self.__class__(itertoolz.nth(n, self.data))

    def get(self, ind, default="__no__default__"):
        return self.__class__(itertoolz.get(ind, self.data, default))

    def cons(self, el):
        return self.__class__(itertoolz.cons(el, self.data))

    def interpose(self, el):
        return self.__class__(itertoolz.interpose(el, self.data))

    def sliding_window(self, n):
        return self.__class__(itertoolz.sliding_window(n, self.data))

    def partition(self, n, pad="__no__pad__"):
        return self.__class__(itertoolz.partition(n, self.data, pad))

    def partition_all(self, n):
        return self.__class__(itertoolz.partition_all(n, self.data))

    def pluck(self, ind, default="__no__default__"):
        return self.__class__(itertoolz.pluck(ind, self.data, default))

    def tail(self, n):
        return self.__class__(itertoolz.tail(n, self.data))

    def topk(self, k, key=None):
        return self.__class__(itertoolz.topk(k, self.data, key))

    def peekn(self, n):
        return self.__class__(itertoolz.peekn(n, self.data))

    def random_sample(self, prob, random_state=None):
        return self.__class__(itertoolz.random_sample(prob, self.data, random_state))

    def countby(self, key):
        return self.__class__(recipes.countby(key, self.data))

    def partitionby(self, func):
        return self.__class__(recipes.partitionby(func, self.data))

    def compact(self, func=None):
        from toolz import filter as _filter

        return self.__class__(_filter(func, self.data))

    def clear(self):
        return []

    def sort(self):
        return self.data.copy().sort()

    def remove(self, value):
        return self.data.copy().remove(value)

    def pop(self, index=-1):
        return self.data.copy().pop(index)


class ROEnhanced_DataSeq(UserList):
    """experimental"""

    def __init__(self, data=None):
        self.data = tuple(data) if data != None else ()

    def pipe(self, *funcs):
        return self.__class__(functoolz.pipe(self.data, *funcs))

    @property
    def count_all(self):
        return self.__class__(itertoolz.count(self.data))

    @property
    def peek(self):
        return self.__class__(itertoolz.peek(self.data))

    @property
    def first(self):
        return self.__class__(itertoolz.first(self.data))

    @property
    def second(self):
        return self.__class__(itertoolz.second(self.data))

    @property
    def last(self):
        return self.__class__(itertoolz.last(self.data))

    def do(self, func):
        return self.__class__(functoolz.do(func, self.data))

    def remove(self, predicate):
        return self.__class__(itertoolz.remove(predicate, self.data))

    def accumulate(self, binop, initial="__no__default__"):
        return self.__class__(itertoolz.accumulate(binop, self.data, initial))

    def groupby(self, key):
        return self.__class__(itertoolz.groupby(key, self.data))

    def interleave(self, seqs):
        return self.__class__(itertoolz.interleave(self.data, seqs))

    def isdistinct(self, seq):
        return self.__class__(itertoolz.isdistinct(self.data))

    def take(self, n):
        return self.__class__(itertoolz.take(n, self.data))

    def drop(self, n):
        return self.__class__(itertoolz.drop(n, self.data))

    def take_nth(self, n):
        return self.__class__(itertoolz.take_nth(n, self.data))

    def nth(self, n):
        return self.__class__(itertoolz.nth(n, self.data))

    def get(self, ind, default="__no__default__"):
        return self.__class__(itertoolz.get(ind, self.data, default))

    def cons(self, el):
        return self.__class__(itertoolz.cons(el, self.data))

    def interpose(self, el):
        return self.__class__(itertoolz.interpose(el, self.data))

    def sliding_window(self, n):
        return self.__class__(itertoolz.sliding_window(n, self.data))

    def partition(self, n, pad="__no__pad__"):
        return self.__class__(itertoolz.partition(n, self.data, pad))

    def partition_all(self, n):
        return self.__class__(itertoolz.partition_all(n, self.data))

    def pluck(self, ind, default="__no__default__"):
        return self.__class__(itertoolz.pluck(ind, self.data, default))

    def tail(self, n):
        return self.__class__(itertoolz.tail(n, self.data))

    def topk(self, k, key=None):
        return self.__class__(itertoolz.topk(k, self.data, key))

    def peekn(self, n):
        return self.__class__(itertoolz.peekn(n, self.data))

    def random_sample(self, prob, random_state=None):
        return self.__class__(itertoolz.random_sample(prob, self.data, random_state))

    def countby(self, key):
        return self.__class__(recipes.countby(key, self.data))

    def partitionby(self, func):
        return self.__class__(recipes.partitionby(func, self.data))

    def compact(self, func=None):
        from toolz import filter as _filter

        return self.__class__(_filter(func, self.data))

    def clear(self):
        return ()

    def sort(self, key=None, reverse=False):
        return tuple(sorted(self.data, key=key, reverse=reverse))


@dataclass
class Enhanced_DataDict(UserDict):
    def __init__(self, data):
        self.data = data

    def pipe(self, *funcs):
        return self.__class__(functoolz.pipe(self.data, *funcs))

    def iget(self, ind, default="__no__default__"):
        return itertoolz.get(ind, self.data, default)

    def itemmap(self, func):
        return self.__class__(dicttoolz.itemmap(func, self.data))

    def keymap(self, func):
        return self.__class__(dicttoolz.keymap(func, self.data))

    def valmap(self, func):
        return self.__class__(dicttoolz.valmap(func, self.data))

    def itemfilter(self, predicate):
        return self.__class__(dicttoolz.itemfilter(predicate, self.data))

    def keyfilter(self, predicate):
        return self.__class__(dicttoolz.keyfilter(predicate, self.data))

    def valfilter(self, predicate):
        return self.__class__(dicttoolz.valfilter(predicate, self.data))

    def merge(self, *dicts, **kwargs):
        return self.__class__(dicttoolz.merge(self.data, *dicts, **kwargs))

    def merge_with(self, func, *dicts, **kwargs):
        return self.__class__(dicttoolz.merge_with(func, self.data, *dicts, **kwargs))

    def update_in(self, keys, func, default=None):
        return self.__class__(dicttoolz.update_in(self.data, keys, func, default))

    def assoc(self, key, value):
        return self.__class__(dicttoolz.assoc(self.data, key, value))

    def assoc_in(self, keys, value):
        return self.__class__(dicttoolz.assoc_in(self.data, keys, value))

    def dissoc(self, *keys, **kwargs):
        return self.__class__(dicttoolz.dissoc(self.data, *keys, **kwargs))

    def pick(self, seq):
        return self.__class__(dicttoolz.keyfilter(lambda k: k in seq, self.data))

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
class ROEnhanced_DataDict(UserDict):
    def __init__(self, data):
        self.data = MappingProxyType(data)

    def pipe(self, *funcs):
        return self.__class__(functoolz.pipe(self.data, *funcs))

    def iget(self, ind, default="__no__default__"):
        return itertoolz.get(ind, self.data, default)

    def itemmap(self, func):
        return self.__class__(dicttoolz.itemmap(func, self.data))

    def keymap(self, func):
        return self.__class__(dicttoolz.keymap(func, self.data))

    def valmap(self, func):
        return self.__class__(dicttoolz.valmap(func, self.data))

    def itemfilter(self, predicate):
        return self.__class__(dicttoolz.itemfilter(predicate, self.data))

    def keyfilter(self, predicate):
        return self.__class__(dicttoolz.keyfilter(predicate, self.data))

    def valfilter(self, predicate):
        return self.__class__(dicttoolz.valfilter(predicate, self.data))

    def merge(self, *dicts, **kwargs):
        return self.__class__(dicttoolz.merge(self.data, *dicts, **kwargs))

    def merge_with(self, func, *dicts, **kwargs):
        return self.__class__(dicttoolz.merge_with(func, self.data, *dicts, **kwargs))

    def update_in(self, keys, func, default=None):
        return self.__class__(dicttoolz.update_in(self.data, keys, func, default))

    def assoc(self, key, value):
        return self.__class__(dicttoolz.assoc(self.data, key, value))

    def assoc_in(self, keys, value):
        return self.__class__(dicttoolz.assoc_in(self.data, keys, value))

    def dissoc(self, *keys, **kwargs):
        return self.__class__(dicttoolz.dissoc(self.data, *keys, **kwargs))

    def pick(self, seq):
        return self.__class__(dicttoolz.keyfilter(lambda k: k in seq, self.data))

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def items(self):
        return self.data.items()

    def clear(self):
        return {}


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


@dataclass
class ROEnhanced_DataChain(ROEnhanced_DataDict):  # partially read-only
    def __init__(self, data, enable_nonlocal=False, parent=None):
        self.parent = parent
        self.enable_nonlocal = enable_nonlocal
        self.data = data
        self.maps = Enhanced_DataSeq([self.data])
        if parent is not None:
            self.maps.extend(parent.maps)

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
