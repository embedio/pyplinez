from collections.abc import Mapping, Sequence
from itertools import chain
from toolz import dicttoolz, itertoolz, functoolz, recipes


class Enhanced_DataSeq(Sequence):
    def __init__(self, data):
        self.data = tuple(data) 

    def pipe(self, *funcs):
        return self.__class__(functoolz.pipe(self.data, *funcs))

    def map(self, func):
        from toolz import map as _map

        return self.__class__(_map(func, self.data))

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

    def countby(self, key):
        return self.__class__(recipes.countby(key, self.data))

    def partitionby(self, func):
        return self.__class__(recipes.partitionby(func, self.data))

    def compact(self, func=None):
        from toolz import filter as _filter

        return self.__class__(_filter(func, self.data))

    def concat(self, seqs):
        return self.__class__(itertoolz.concat([self.data, seqs]))

    def clear(self):
        return []

    def __len__(self):
        return itertoolz.count(self.data)

    def __getitem__(self, key):
        return tuple(itertoolz.pluck(key, self.data))


class RO_DataChain(Mapping):
    def __init__(self, data, enable_nonlocal=False, parent=None):
        self.parent = parent
        self.enable_nonlocal = enable_nonlocal
        self.data = data 
        self.maps = Enhanced_DataSeq([self.data])
        if parent is not None:
            self.maps = self.maps.concat([parent.maps])

    def new_child(self, data, enable_nonlocal=None):
        enable_nonlocal = (
            self.enable_nonlocal if enable_nonlocal is None else enable_nonlocal
        )
        return self.__class__(data=data, enable_nonlocal=enable_nonlocal, parent=self)

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def items(self):
        return self.data.items()

    def clear(self):
        return {}

    def get(self, *keys):
        return self.data.__getitem__(*keys)

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

    @property
    def root(self):
        return self if self.parent is None else self.parent.root

    def __getitem__(self, key):
        return tuple(itertoolz.pluck(key, self.maps.data))

    def __setitem__(self, key, value):
        if self.enable_nonlocal:
            set_value = lambda d: dicttoolz.assoc(d, key=key, value=value)
            return [set_value(d) for d in self.maps.data]
        dicttoolz.assoc(self.data, key=key, value=value)

    def __delitem__(self, key):
        if self.enable_nonlocal:
            unset_value = lambda d: dicttoolz.dissoc(d, key=key)
            return [unset_value(d) for d in self.maps.data]
        dicttoolz.dissoc(self.data, key=key)

    def __len__(self):
        return itertoolz.count(self.maps.data)

    def __iter__(self, chain_from_iterable=chain.from_iterable):
        return chain_from_iterable(self.maps.data)

    def __contains__(self, key, any=any):
        return any(key in m for m in self.maps.data)

    def __repr__(self, repr=repr):
        return " -> ".join(map(repr, self.maps.data))