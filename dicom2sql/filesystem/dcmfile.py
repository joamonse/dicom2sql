#Must need to be processed in order to work
from __future__ import annotations

import logging
from pathlib import Path

import pydicom
from pydicom.errors import InvalidDicomError

from dicom2sql.config_file import ConfigFile


class DcmFile:
    def __init__(self, config:ConfigFile, path:Path, line_num: int | None=None):
        self.path = path
        self.config = config
        self.loaded = False
        self.loading = False
        self.error = False
        self.dcm_data = None
        self.line_num=line_num

    def load(self):
        self.loading = True
        try:
            self.dcm_data = pydicom.dcmread(self.path, stop_before_pixels=True)
        except InvalidDicomError:
            logging.getLogger("dicom2sql").error(f'{self.path} contains error or is not a dicom')
            self.error = True

        except TypeError:
            logging.getLogger("dicom2sql").warning(f'{self.path} is not dicom')
            self.error = True

        except FileNotFoundError:
            logging.getLogger("dicom2sql").warning(f'{self.path} does not exist')
            self.error = True

        self.loading = False
        self.loaded = True

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            if self.line_num:
                self.config.set_last_file(self.line_num)
            else:
                self.config.set_last_file(self.path)

    def __repr__(self):
        return self.path.__repr__()