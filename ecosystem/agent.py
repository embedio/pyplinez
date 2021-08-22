import pickle, json, csv, os, shutil, shelve
from pathlib import Path
from dataclasses import dataclass
from collections.abc import Mapping, Callable
from types import MappingProxyType
from pyptoolz.pypstructz import DataChain, PersistentDict


@dataclass
class Agent:
    def __init__(self, env=None, storage="/tmp/ephemoral.pickle", *args, **kwargs):
        self._memory = PersistentDict(
            filename=storage, flag="c", mode=None, format="pickle"
        )
        self._env = (
            env
            if env != None and isinstance(env, Environment)
            else Environment(location=env, agent=self, *args, **kwargs)
        )
        if self._env.env_data:
            self._info_file = PersistentDict(self._env.env_data.get("info"))
            self._func_file = PersistentDict(self._env.env_data.get("func"))

    @property
    def memory(self):
        return self._memory.keys()

    @property
    def info_cache(self):
        return self._info_file.keys()

    @property
    def func_cache(self):
        return self._func_file.keys()

    @property
    def resources(self):
        return DataChain(self._env.resources)

    @property
    def location(self):
        return self._env.location

    @property
    def id(self):
        return id(self)

    def pickle_info(self, info, info_name=""):
        if not isinstance(info, Mapping):
            return (info,)
        info_name = info_name if len(info_name) > 0 else str(id(info))
        self._info_file.format = pickle
        return self._info_file.update({info_name: info})

    def dill_func(self, func, func_name=""):
        if not isinstance(func, Callable):
            return (func,)
        func_name = func_name if len(func_name) > 0 else str(id(func))
        self._func_file.format = "dill"
        return self._func_file.update({func_name: func})

    def memorize(self, info, info_name="", format="pickle"):
        if not isinstance(info, Mapping):
            return (info,)
        info_name = info_name if len(info_name) > 0 else str(id(info))
        self._memory.format = format
        return self._memory.update({info_name: info})

    def from_storage(self, key, filename=None):
        if filename is None:
            return self._memory.get(key)
        elif filename == "func":
            return self._func_file.get(key)
        elif filename == "info":
            return self._info_file.get(key)
        else:
            return (key, filename)

    def remove_storage(self, filename):
        if filename == "memory":
            return self._memory.clear()
        elif filename == "func":
            return self._func_file.clear()
        elif filename == "info":
            return self._info_file.clear()
        else:
            return (filename,)

    def sync_storage(self, filename=None):
        if filename is None:
            return self._memory.sync()
        elif filename == "func":
            return self._func_file.sync()
        elif filename == "info":
            return self._info_file.sync()
        else:
            return (filename,)


@dataclass
class Environment:
    def __init__(self, location=None, agent=None, *args, **kwargs):
        self.location = Path(location) if location != None else Path.cwd()
        self.resources = (
            MappingProxyType({path.stem: path for path in self.location.iterdir()})
            if self.location.is_dir()
            else location
        )
        self.env_data = MappingProxyType(
            {path.stem: path for path in self.resources.get(".env_data").iterdir()}
        )
        self._agent = (
            agent
            if agent != None and isinstance(agent, Agent)
            else Agent(env=self.location, *args, *kwargs)
        )

    @property
    def id(self):
        return id(self)
