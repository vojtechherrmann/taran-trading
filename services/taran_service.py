import json
import os
from shutil import copyfile
import logging
from datetime import datetime
from pathlib import Path
from abc import ABC
from typing import List

from utilities.utils import convert_camel_to_snake


class TaranService(ABC):

    _logs_path = Path("logs")
    _current_log_name = "_current.log"

    @property
    def appsettings_attrs_mandatory_error(self) -> List[str]:
        return []

    @property
    def appsettings_attrs_mandatory_warning(self) -> List[str]:
        return ["tickers"]

    def _archive_current_logger(self, as_error: bool = False, delete_current: bool = True) -> None:
        if self._current_log_name in os.listdir(self._logs_path):
            old_file_name = self._logs_path / Path(self._current_log_name)
            new_file_name = self._logs_path / Path(f"log{'_error' if as_error else ''}_{self._now_str}.log")
            copyfile(src=old_file_name, dst=new_file_name)
            if delete_current:
                os.remove(old_file_name)

    def _init_logger(self) -> None:

        # TODO
        # opened log means error in the last run
        # if self._current_log_name in os.listdir(self._logs_path):
        #     self._archive_current_logger(as_error=True)

        # new current logger
        logging.basicConfig(
            filename=f"logs/{self._current_log_name}",
            level=logging.INFO,
            filemode='a',
            format='%(asctime)s %(name)s %(message)s',
        )

    def _export_logger(self) -> None:
        logging.shutdown()
        self._archive_current_logger()

    def _load_and_unpack_appsettings(self, filename: str = "appsettings.json") -> None:

        attrs_mandatory_error = self.appsettings_attrs_mandatory_error
        attrs_mandatory_warning = self.appsettings_attrs_mandatory_warning

        # first we load general appsettings, then service-specific
        with open(f"{filename}", 'r') as f:
            appsettings = json.load(f)
        service_appsettings_filename = self.service_folder / Path(filename)
        if not os.path.exists(service_appsettings_filename) or not os.path.isfile(service_appsettings_filename):
            logging.warning(f"No service specific appsettings found. Path does not exist: {service_appsettings_filename}/{filename}")
        else:
            with open(f"{service_appsettings_filename}", 'r') as f:
                appsettings.update(json.load(f))

        for _attr in attrs_mandatory_error:
            if _attr in appsettings.keys():
                self.__setattr__(_attr, appsettings[_attr])
            else:
                raise RuntimeError(f"No {_attr} provided in appsettings")

        for _attr in attrs_mandatory_warning:
            if _attr in appsettings.keys():
                self.__setattr__(_attr, appsettings[_attr])
            else:
                logging.warning(f"No {_attr} provided in appsettings")

        attrs_other = set(appsettings.keys()) - set(attrs_mandatory_error) - set(attrs_mandatory_warning)
        for _attr in attrs_other:
            if _attr in appsettings.keys():
                self.__setattr__(_attr, appsettings[_attr])

    def __init__(self):
        self._now_str = datetime.now().strftime("%Y_%m_%d_%H%M%S")
        self._init_logger()

        # appsettings
        self.tickers: List[str] = []
        self._load_and_unpack_appsettings()

    def __del__(self):
        self._export_logger()

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def name_snake_case(self) -> str:
        return convert_camel_to_snake(self.name)

    @property
    def service_folder(self) -> Path:
        return Path("services") / Path(self.name_snake_case)
