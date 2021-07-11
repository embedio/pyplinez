from collections import UserList
from dataclasses import dataclass
from .. import functoolz, itertoolz, recipes


@dataclass
class DataSeq(UserList):
    def __init__(self, data=None):
        self.data = tuple(data)

    def pipe(self, *funcs):
        return DataSeq(functoolz.pipe(self.data, *funcs))

    def remove(self, predicate):
        return DataSeq(itertoolz.remove(predicate, self.data))

    def accumulate(self, binop, initial="__no__default__"):
        return DataSeq(itertoolz.accumulate(binop, self.data, initial))

    def groupby(self, key):
        return DataSeq(itertoolz.groupby(key, self.data))

    def interleave(self, seqs):
        return DataSeq(itertoolz.interleave(self.data, seqs))

    def unique(self, key=None):
        return DataSeq(itertoolz.unique(self.data, key))

    def isdistinct(self, seq):
        return DataSeq(itertoolz.isdistinct(self.data))

    def take(self, n):
        return DataSeq(itertoolz.take(n, self.data))

    def drop(self, n):
        return DataSeq(itertoolz.drop(n, self.data))

    def take_nth(self, n):
        return DataSeq(itertoolz.take_nth(n, self.data))

    def first(self):
        return DataSeq(itertoolz.first(self.data))

    def first(self):
        return DataSeq(itertoolz.first(self.data))

    def second(self):
        return DataSeq(itertoolz.second(self.data))

    def nth(self, n):
        return DataSeq(itertoolz.nth(n, self.data))

    def last(self, n):
        return DataSeq(itertoolz.last(self.data))

    def get(self, ind, default="__no__default__"):
        return DataSeq(itertoolz.get(ind, self.data, default))

    def cons(self, el):
        return DataSeq(itertoolz.cons(el, self.data))

    def interpose(self, el):
        return DataSeq(itertoolz.interpose(el, self.data))

    def frequencies(self):
        pass

    def reduceby(self):
        pass

    def sliding_window(self, n):
        return DataSeq(itertoolz.sliding_window(n, self.data))

    def partition(self, n, pad="__no__pad__"):
        return DataSeq(itertoolz.partition(n, self.data, pad))

    def partition_all(self, n):
        return DataSeq(itertoolz.partition_all(n, self.data))

    def count(self):
        return DataSeq(itertoolz.count(self.data))

    def pluck(self, ind, default="__no__default__"):
        return DataSeq(itertoolz.pluck(ind, self.data, default))

    def join(self):
        pass

    def tail(self, n):
        return DataSeq(itertoolz.tail(n, self.data))

    def diff(self):
        pass

    def topk(self, k, key=None):
        return DataSeq(itertoolz.topk(k, self.data, key))

    def peek(self):
        return DataSeq(itertoolz.peek(self.data))

    def peekn(self, n):
        return DataSeq(itertoolz.peekn(n, self.data))

    def random_sample(self, prob, random_state=None):
        return DataSeq(itertoolz.random_sample(prob, self.data, random_state))

    def countby(self, key):
        return DataSeq(recipes.countby(key, self.data))

    def partitionby(self, func):
        return DataSeq(recipes.partitionby(func, self.data))
