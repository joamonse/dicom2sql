#Must need to be processed in order to work
import logging

import pydicom
from pydicom.errors import InvalidDicomError


class DcmFile:
    def __init__(self, config, path):
        self.path = path
        self.config = config
        self.loaded = False
        self.loading = False
        self.error = False
        self.dcm_data = None

    def load(self):
        self.loading = True
        try:
            self.dcm_data = pydicom.dcmread(self.path, stop_before_pixels=True)
        except InvalidDicomError:
            logging.getLogger(__name__).error(f'{self.path} contains error or is not a dicom')
            self.error = True

        except TypeError:
            logging.getLogger(__name__).warning(f'{self.path} is not dicom')
            self.error = True

        except FileNotFoundError:
            logging.getLogger(__name__).warning(f'{self.path} does not exist')
            self.error = True

        self.loading = False
        self.loaded = True

    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        if not exc_type:
            self.config.set_last_file(self.path)

    def __repr__(self):
        return self.path.__repr__()