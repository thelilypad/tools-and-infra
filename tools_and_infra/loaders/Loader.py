import abc
from functools import wraps

from tools_and_infra.config import Config, ConfigValue


class LoaderException(Exception):
    pass


class Loader(abc.ABC):
    def __init__(self):
        self.api_key: ConfigValue = Config.get_property(self.api_key_property())

    @staticmethod
    def _raise_api_exception_if_missing_config(method):
        def wrapper(self, *arg, **kw):
            if self.api_key.is_empty():
                raise LoaderException("Loader for [" + self.service_name() + "] missing API Key.")
            res = method(self, *arg, **kw)
            return res
        return wrapper

    @abc.abstractmethod
    def api_key_property(self) -> str:
        pass

    @abc.abstractmethod
    def service_name(self) -> str:
        pass
