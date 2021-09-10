from pathlib import Path
from dataclasses import dataclass
from collections.abc import Mapping, Callable
from types import MappingProxyType
from pyptoolz.pypstructz import DataChain, PersistentDict


@dataclass
class Agent:
    def __init__(self, env=None, storage="/tmp/ephemoral.pkl", **kwargs):
        self._memory = PersistentDict(
            filename=storage, flag="c", mode=None, format="dill"
        )
        self._env = (
            env
            if env != None and isinstance(env, Environment)
            else Environment(location=env, agent=self, **kwargs)
        )

    @property
    def memory(self):
        return self._memory.keys()

    @property
    def info_cache(self):
        return self._env.cache.get("info").keys()

    @property
    def func_cache(self):
        return self._env.cache.get("func").keys()

    @property
    def resources(self):
        return DataChain(self._env.resources)

    @property
    def _location(self):
        return self._env.location

    @property
    def _id(self):
        return id(self)

    def cache_info(self, info, info_name="", format_="dill"):
        if not isinstance(info, DataChain):
            return (info,)
        info_name = info_name if len(info_name) > 0 else str(id(info))
        self._env.cache.get("info").format = format_
        return self._env.cache.get("info").update({info_name: info})

    def cache_func(self, func, func_name="", format_="dill"):
        if not isinstance(func, Callable):
            return (func,)
        func_name = func_name if len(func_name) > 0 else str(id(func))
        self._env.cache.get("func").format = format_
        return self._env.cache.get("func").update({func_name: func})

    def memorize(self, info, info_name="", format_="dill"):
        if not isinstance(info, Mapping):
            return (info,)
        info_name = info_name if len(info_name) > 0 else str(id(info))
        self._memory.format = format_
        return self._memory.update({info_name: info})

    def get(self, key, filename="memory"):
        if filename == "memory":
            return self._memory.get(key)
        elif filename in ["func", "info"]:
            return self._env.cache.get(filename).get(key)
        else:
            return (filename,)

    def _sync(self, filename="memory"):
        if filename == "memory":
            return self._memory.sync()
        elif filename in ["func", "info"]:
            return self._env.cache.get(filename).sync()
        else:
            return (filename,)

    def _clear(self, key, filename):
        if filename == "memory" and key == "clearmem":
            return self._memory.clear()
        elif (
            filename == "func"
            and key == "clearfun"
            or filename == "info"
            and key == "clearinf"
        ):
            return self._env.cache.get(filename).clear()
        else:
            return (
                key,
                filename,
            )

    def _pop(self, key, filename):
        if filename == "memory":
            return self._memory.pop(key)
        elif filename in ["func", "info"]:
            return self._env.cache.get(filename).pop(key)
        else:
            return (
                key,
                filename,
            )

    def _popitem(self, filename):
        if filename == "memory":
            return self._memory.popitem()
        elif filename in ["func", "info"]:
            return self._env.cache.get(filename).popitem()
        else:
            return (filename,)


@dataclass
class Environment:
    def __init__(self, location=None, cache=".env_data", agent=None, **kwargs):
        self.location = Path(location) if location != None else Path.cwd()
        self.resources = MappingProxyType(
            {
                path.stem: path
                for path in self.location.iterdir()
                if self.location.is_dir()
            }
        )
        self.cache = MappingProxyType(
            {
                path.stem: PersistentDict(path.as_posix(), format="dill")
                for path in self.resources.get(cache).iterdir()
            }
        )
        self._agent = (
            agent
            if agent != None and isinstance(agent, Agent)
            else Agent(env=self.location, **kwargs)
        )

    @property
    def id(self):
        return id(self)
