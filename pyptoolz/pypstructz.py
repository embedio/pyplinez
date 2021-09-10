import pickle, json, csv, os, shutil
from pathlib import Path
from dataclasses import dataclass
from collections.abc import Mapping, Sequence
from types import MappingProxyType
from itertools import chain
import dill
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


@dataclass
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

    @property
    def reverse(self):
        return self.__class__(reversed(self.data))

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


@dataclass
class DataChain(Mapping):
    def __init__(self, data=None, parent=None):
        self.parent = parent
        self.data = (
            MappingProxyType(data) if data != None and isinstance(data, Mapping) else {}
        )
        # self._data = MappingProxyType(self.data)
        self._data = self.data
        self.maps = DataSeq([self.data])
        if parent is not None:
            fam_maps = self.maps + parent.maps
            self.maps = DataSeq(fam_maps)

    def new_child(self, data):
        return self.__class__(data=data, parent=self)

    @property
    def root(self):
        return self if self.parent is None else self.parent.root

    @property
    def generations(self):
        return MappingProxyType(dict(enumerate(reversed(self.maps.data))))

    @property
    def keys(self):
        return self.data.keys()

    @property
    def values(self):
        return self.data.values()

    @property
    def items(self):
        return self.data.items()

    def ddpipe(self, *funcs):
        return self.new_child(functoolz.pipe(self._data, *funcs))

    def ddrec(self, *funcs):
        for func in funcs:
            self = self.new_child(func(self._data))
        return self

    def do(self, func):
        return self.new_child(functoolz.do(func, self._data))

    def get(self, key, default=None):
        return self.data.get(key, default)

    def getz(self, ind, default="__no__default__"):
        return itertoolz.get(ind, self.data, default)

    def get_in(self, keys, default=None, no_default=False):
        return tuple(get_in(keys, self.data, default=default, no_default=no_default))

    def valmap(self, func):
        return self.new_child(dicttoolz.valmap(func, self._data))

    def valfilter(self, predicate):
        return self.new_child(dicttoolz.valfilter(predicate, self._data))

    def keymap(self, func):
        return self.new_child(dicttoolz.keymap(func, self._data))

    def keyfilter(self, predicate):
        return self.new_child(dicttoolz.keyfilter(predicate, self._data))

    def itemmap(self, func):
        return self.new_child(dicttoolz.itemmap(func, self._data))

    def itemfilter(self, predicate):
        return self.new_child(dicttoolz.itemfilter(predicate, self._data))

    def assoc(self, key, value):
        return self.new_child(dicttoolz.assoc(self._data, key, value))

    def assoc_in(self, keys, value):
        return self.new_child(dicttoolz.assoc_in(self._data, keys, value))

    def dissoc(self, *keys, **kwargs):
        return self.new_child(dicttoolz.dissoc(self._data, *keys, **kwargs))

    def merge(self, *dicts, **kwargs):
        return self.new_child(dicttoolz.merge(self._data, *dicts, **kwargs))

    def merge_with(self, func, *dicts, **kwargs):
        return self.new_child(dicttoolz.merge_with(func, self._data, *dicts, **kwargs))

    def pick(self, keys):
        p = functoolz.partial(lambda k: k in keys)
        return self.new_child(dicttoolz.keyfilter(p, self._data))

    def ignore(self, keys):
        i = functoolz.partial(lambda k: k not in keys)
        return self.new_child(dicttoolz.keyfilter(i, self._data))

    def __getitem__(self, key):
        return tuple(itertoolz.pluck(key, self.maps.data))

    def __len__(self):
        return itertoolz.count(self.maps.data)

    def __iter__(self, chain_from_iterable=chain.from_iterable):
        return chain_from_iterable(self.maps.data)

    def __contains__(self, key, any=any):
        return any(key in m for m in self.maps.data)

    def __repr__(self, repr=repr):
        return " ANCESTOR --> ".join(map(repr, self.maps))


@dataclass
class PersistentDict(dict):
    """Persistent dictionary with an API compatible with shelve and anydbm.

    The dict is keypt in memory, so the dictionary operations run as fast as a regular dict.

    Write to disk is delayed until close or sync (similar to gdbm's fast mode).

    Input file format is automatically discovered.
    Output file format is selectable between pickle, json, and csv.
    All three serialization formats are backed by fast C implementations.
    """

    def __init__(self, filename, flag="c", mode=None, format="pickle", *args, **kwargs):
        self.flag = flag  # r=readoly, c=create, or n=new
        self.mode = mode  # None or an octal triple like 0644
        self.format = format  # 'csv', 'json', or 'pickle'
        self.filename = filename
        if flag != "n" and os.access(filename, os.R_OK):
            fileobj = open(filename, "rb" if format == "pickle" or "dill" else "r")
            with fileobj:
                self.load(fileobj)
        dict.__init__(self, *args, **kwargs)

    def sync(self):
        "Write dict to disk"
        if self.flag == "r":
            return
        filename = Path(self.filename).as_posix()
        tempname = filename + ".tmp"
        fileobj = open(tempname, "wb" if self.format == "pickle" or "dill" else "w")
        try:
            self.dump(fileobj)
        except Exception:
            os.remove(tempname)
            raise
        finally:
            fileobj.close()
        shutil.move(tempname, self.filename)  # atomic commit
        if self.mode is not None:
            os.chmod(self.filename, self.mode)

    def close(self):
        self.sync()

    def __enter__(self):
        return self

    def __exit__(self):
        self.close()

    def dump(self, fileobj):
        if self.format == "csv":
            csv.writer(fileobj).writerows(self.items())
        elif self.format == "json":
            json.dump(self, fileobj, seperators=(",", ":"))
        elif self.format == "pickle":
            pickle.dump(dict(self), fileobj, 2)
        elif self.format == "dill":
            dill.dump(dict(self), fileobj, 4)
        else:
            raise NotImplementedError("Unknown format: " + repr(self.format))

    def load(self, fileobj):
        # try formats from most restrictive to least
        # for loader in (pickle.load, json.load, csv.reader):
        for loader in (pickle.load, dill.load, json.load, csv.reader):
            fileobj.seek(0)
            try:
                return self.update(loader(fileobj))
            except Exception:
                pass
        raise ValueError("File not in a supported format")

    def fromkeys(self, iterable, value=None):
        print("Not Implemented")

    def copy(self):
        print("Not Implemented")
