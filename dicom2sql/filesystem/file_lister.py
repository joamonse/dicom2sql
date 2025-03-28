from pathlib import Path
from typing import Generator

from dicom2sql.config_file import ConfigFile
from .dcmfile import DcmFile


def get_files_from_list(save_file: Path) -> Generator[DcmFile, None, None]:
    config_file = ConfigFile(save_file)

    with open(save_file) as file_list, config_file:
        last_line = config_file.get_last_file()
        if not last_line:
            last_line = 0

        for i in range(last_line):
            file_list.readline()

        for line in file_list:
            path = Path(line.strip())
            context = DcmFile(config_file,path)
            yield context

            last_line += 1

        config_file.remove()