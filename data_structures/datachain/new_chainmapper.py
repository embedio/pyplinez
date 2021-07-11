from collections import MutableMapping
from itertools import chain

class Context(MutableMapping):

    def __init__(self, enable_nonlocal=False, parent=None):
        'Create a new root context'
        self.parent = parent
        self.enable_nonlocal = enable_nonlocal
        self.map = {}
        self.maps = [self.map]
        if parent is not None:
            self.maps += parent.maps

    def new_child(self, enable_nonlocal=None):
        'Make a child context, inheriting enable_nonlocal unless specified'
        enable_nonlocal = self.enable_nonlocal if enable_nonlocal is None else enable_nonlocal
        return self.__class__(enable_nonlocal=enable_nonlocal, parent=self)

    @property
    def root(self):
        'Return root context (highest level ancestor)'
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

    def __delitem__(self, key):
        if self.enable_nonlocal:
            for m in self.maps:
                if key in m:
                    del m[key]
                    return
        del self.map[key]

    def __len__(self, len=len, sum=sum, map=map):
        return sum(map(len, self.maps))

    def __iter__(self, chain_from_iterable=chain.from_iterable):
        return chain_from_iterable(self.maps)

    def __contains__(self, key, any=any):
        return any(key in m for m in self.maps)

    def __repr__(self, repr=repr):
        return ' -> '.join(map(repr, self.maps))

if __name__ == '__main__':
    c = Context()
    c['a'] = 1
    c['b'] = 2
    d = c.new_child()
    d['c'] = 3
    print('d:  ', d)
    assert repr(d) == "{'c': 3} -> {'a': 1, 'b': 2}"

    e = d.new_child()
    e['d'] = 4
    e['b'] = 5
    print('e:  ', e)
    assert repr(e) == "{'b': 5, 'd': 4} -> {'c': 3} -> {'a': 1, 'b': 2}"

    f = d.new_child(enable_nonlocal=True)
    f['d'] = 4
    f['b'] = 5
    print('f:  ', f)
    assert repr(f) == "{'d': 4} -> {'c': 3} -> {'a': 1, 'b': 5}"

    print(len(f))
    assert len(f) == 4
    assert len(list(f)) == 4
    assert all(k in f for k in f)
    assert f.root == c

    # dynanmic scoping example
    def f(ctx):
        print(ctx['a'], 'f:  reading "a" from the global context')
        print('f: setting "a" in the global context')
        ctx['a'] *= 999
        print('f: reading "b" from globals and setting "c" in locals')
        ctx['c'] = ctx['b'] * 50
        print('f: ', ctx)
        g(ctx.new_child())
        print('f: ', ctx)


    def g(ctx):
        print('g: setting "d" in the local context')
        ctx['d'] = 44
        print('''g: setting "c" in f's context''')
        ctx['c'] = -1
        print('g: ', ctx)
    global_context = Context(enable_nonlocal=True)
    global_context.update(a=10, b=20)
    f(global_context.new_child())
