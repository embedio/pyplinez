from collections import UserDict, UserList

class Protected_UserDict(UserDict):
    def __init__(self, data):
        self.data = data


    def clear(mapping):
        pass


    def pop(key):
        pass


    def popitem(key, value):
        pass


    def update(other):
        pass


    def setdefault():
        pass


    def fromkeys():
        pass


    def copy():
        pass


class Protected_UserList(UserList):
    def __init__(self, data):
        self.data = data

    def append(self):
        pass


    def extend(self):
        pass


    def insert(self):
        pass


    def remove(self):
        pass


    def reverse(self):
        pass


    def sort(self):
        pass

    def __add__(self):
        pass


    def __iadd__(self):
        pass


    def __mul__(self):
        pass


    def __imul__(self):
        pass


    def __rmul__(self):
        pass


    def __reversed__(self):
        pass


def __setitem__():
    pass


def __setattr__():
    pass


def __delitem__():
    pass


def __delattr__():
    pass
