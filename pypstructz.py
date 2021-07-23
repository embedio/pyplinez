from collections import ChainMap
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from itertools import chain
from toolz import (
    dicttoolz,
    itertoolz,
    functoolz,
    recipes,
    curried,
    get_in,
    map as mapz,
    filter as filterz,
)


class DataSeq(Sequence):
    def __init__(self, data):
        self.data = tuple(data)

    def dspipe(self, *funcs):
        vp = lambda d: functoolz.pipe(d, *funcs)
        return self.mapz(vp)

    def mapz(self, func):
        return self.__class__(list(mapz(func, self.data)))

    @property
    def clear(self):
        return ()

    @property
    def count(self):
        return self.data.__len__()

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

    def sort(self, key=None, reverse=False):
        return self.__class__(sorted(self.data, key=key, reverse=reverse))

    def do(self, func):
        return functoolz.do(func, self.data)

    def remove(self, predicate):
        return self.__class__(itertoolz.remove(predicate, self.data))

    def accumulate(self, binop, initial="__no__default__"):
        return self.__class__(itertoolz.accumulate(binop, self.data, initial))

    def groupby(self, key):
        return self.__class__(itertoolz.groupby(key, self.data))

    def interleave(self, *seqs):
        return self.__class__(itertoolz.interleave(self.data, *seqs))

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

    def reduceby(self, key, binop, init="__no__default__"):
        return self.__class__(
            itertoolz.reduceby(key=key, binop=binop, seq=self.data, init=init)
        )

    def countby(self, key):
        return self.__class__(recipes.countby(key, self.data))

    def partitionby(self, func):
        return self.__class__(recipes.partitionby(func, self.data))

    def compact(self, func=None):
        return self.__class__(filterz(func, self.data))

    def concat(self, seqs):
        return self.__class__(itertoolz.concat([self.data, seqs]))

    def concatv(self, seqs):
        return self.__class__(itertoolz.concatv(self.data, seqs))

    def __len__(self):
        return itertoolz.count(self.data)

    def __getitem__(self, key):
        return itertoolz.get(key, self.data)

    def __repr__(self):
        return f"{self.data}"

    def __add__(self, other):
        return tuple(itertoolz.concatv(self, other))


class DataChain(Mapping):
    def __init__(self, data, enable_nonlocal=False, parent=None):
        self.parent = parent
        self.enable_nonlocal = enable_nonlocal
        # self.data = data
        self.data = MappingProxyType(data)
        self.maps = DataSeq([self.data])
        if parent is not None:
            fam_maps = self.maps + parent.maps
            self.maps = DataSeq(fam_maps)

    def new_child(self, data, enable_nonlocal=None):
        enable_nonlocal = (
            self.enable_nonlocal if enable_nonlocal is None else enable_nonlocal
        )
        return self.__class__(data=data, enable_nonlocal=enable_nonlocal, parent=self)

    @property
    def root(self):
        return self if self.parent is None else self.parent.root

    @property
    def keys(self):
        return self.data.keys()

    @property
    def values(self):
        return self.data.values()

    @property
    def items(self):
        return self.data.items()

    @property
    def clear(self):
        return {}

    def ddpipe(self, *funcs):
        return self.__class__(functoolz.pipe(self.data, *funcs))

    def ddrec(self, *funcs):
        for func in funcs:
            self = self.new_child(func(self.data))
        return self

    def do(self, func):
        return self.__class__(functoolz.do(func, self.data))

    def get(self, ind, default="__no__default__"):
        return tuple(itertoolz.get(ind, self.data, default))

    def get_in(self, keys, default=None, no_default=False):
        return tuple(get_in(keys, self.data, default=default, no_default=no_default))

    def valmap(self, func):
        return self.__class__(dicttoolz.valmap(func, self.data))

    def valfilter(self, predicate):
        return self.__class__(dicttoolz.valfilter(predicate, self.data))

    def keymap(self, func):
        return self.__class__(dicttoolz.keymap(func, self.data))

    def keyfilter(self, predicate):
        return self.__class__(dicttoolz.keyfilter(predicate, self.data))

    def itemmap(self, func):
        return self.__class__(dicttoolz.itemmap(func, self.data))

    def itemfilter(self, predicate):
        return self.__class__(dicttoolz.itemfilter(predicate, self.data))

    def __getitem__(self, key):
        return tuple(itertoolz.pluck(key, self.maps.data))

    def __setitem__(self, key, value):
        if self.enable_nonlocal:
            set_value = lambda d: dicttoolz.assoc(d, key=key, value=value)
            return tuple((set_value(d) for d in self.maps.data))
        dicttoolz.assoc(self.data, key=key, value=value)

    def __delitem__(self, key):
        if self.enable_nonlocal:
            unset_value = lambda d: dicttoolz.dissoc(d, key=key)
            return tuple((unset_value(d) for d in self.maps.data))
        dicttoolz.dissoc(self.data, key=key)

    def __len__(self):
        return itertoolz.count(self.maps.data)

    def __iter__(self, chain_from_iterable=chain.from_iterable):
        return chain_from_iterable(self.maps.data)

    def __contains__(self, key, any=any):
        return any(key in m for m in self.maps.data)

    def __repr__(self, repr=repr):
        return " -> ".join(map(repr, self.maps))


from pathlib import Path


class DataPipe:
    def __init__(self, dir):
        self.source = MappingProxyType({f.stem: f for f in Path(dir).iterdir()})
        self.main = DataChain(self.source)
        self.root = self.main.root


class ChainPipe(ChainMap):
    def __init__(self, data):
        self.data = DataChain(data)
        self.maps = DataSeq(self.data)
