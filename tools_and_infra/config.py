import os
from typing import List
import logging
import yaml

CONFIG_FILE_NAME = 'tools-and-infra-config.yaml'

SPECIFIED_PROPERTIES = [
    'TIINGO_API_KEY',
    'ALPHA_VANTAGE_API_KEY',
    'ORATS_API_KEY',
]


def specifiable_properties():
    return SPECIFIED_PROPERTIES


class ConfigException(Exception):
    pass


class _Config:
    def __init__(self):
        self.__properties__: dict = {}
        if os.path.exists(CONFIG_FILE_NAME):
            with open(CONFIG_FILE_NAME) as f:
                yaml_data_map = yaml.safe_load(f)
                if type(yaml_data_map) is dict:
                    self.__properties__ = yaml_data_map
                else:
                    raise yaml.YAMLError("Provided yaml file needs to be in tree format.")
        else:
            self._load_from_os()

    def _load_from_os(self):
        for key in SPECIFIED_PROPERTIES:
            prop = os.getenv(key)
            if prop:
                path = key.split(".")
                properties_setter = self.__properties__
                while len(path) > 1:
                    elem = path.pop(0)
                    if elem in properties_setter:
                        if not type(properties_setter[elem]) is dict:
                            raise ConfigException("[" + elem + "] exists in config path already and is not dict.")
                        properties_setter = properties_setter[elem]
                    else:
                        properties_setter[elem] = {}
                properties_setter[path[0]] = prop
            else:
                logging.warning("[" + key + "] not set in OS Env!")

    def get_property(self, property_name: str) -> List:
        if property_name not in specifiable_properties():
            logging.warning("Provided property: [" + property_name + "] not in specifiable properties.")
            return []
        path = property_name.split(".")
        properties_accessor = self.__properties__
        for item in path:
            if item not in path:
                logging.warning("Provided property: [" + property_name + "] not found in config.")
            properties_accessor = properties_accessor[item]
        if type(properties_accessor) is list:
            return properties_accessor
        else:
            return [properties_accessor]


Config = _Config()
